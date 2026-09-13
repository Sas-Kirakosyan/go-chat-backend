// k6/read.js is the Stage 8 load test for the REST read path.
//
// It exists because Stage 8 changes how reads are served — an index, a read
// replica, a cache, partitions — and no one of those changes can be defended
// without a number from before it and a number from after it. So this script
// runs first, against the code as it is today, and its output is the baseline
// every later step is compared to.
//
// It does NOT drive WebSockets. cmd/wsload already opens 5000 sockets, honours
// Retry-After and produced the Stage 1 numbers. A second socket tool written in
// JavaScript would measure the same thing worse.
//
// Run it:
//
//	docker compose up --build -d
//	make seed ARGS="-n 50 -rooms 20 -messages 200"
//	make k6
//
// The harder run, which is the one that finds the knee:
//
//	make k6 ARGS="-e PROFILE=ramp"
//
// What to write down: the p50, p95 and p99 of each read_* trend at the bottom
// of the summary. Those five lines are the whole point of the run.
//
// Tuning, all through -e NAME=value:
//
//	PROFILE   steady (default) or ramp
//	RATE      requests per second for the steady profile        (default 100)
//	PEAK      requests per second the ramp profile climbs to    (default 600)
//	DURATION  how long the steady profile holds                 (default 2m)
//	USERS     how many seeded users to log in                   (default 50)
//	ROOM_PREFIX  which rooms count as load rooms                (default loadroom)
//	WRITE_PCT percent of iterations that send a message         (default 10)
//	BASE_URL  what to hit                       (default http://nginx:80 in compose)
//
// A note on the rate limit. Requests behind the auth middleware are limited per
// user id at 20/s, so the offered rate has to stay under USERS * 20 or the test
// measures the limiter instead of the database. The script fails the run if it
// sees a single 429, and the fix is either more users or, for a load run,
// RATE_LIMIT_API_RPS raised in the compose environment. Raising it is honest;
// a back door that skips the limiter would mean load testing a server we do not
// ship.

import http from 'k6/http';
import { check, fail, sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';
import exec from 'k6/execution';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const BASE_URL = __ENV.BASE_URL || 'http://nginx:80';
const USERS = Number(__ENV.USERS || 50);
const PASSWORD = __ENV.PASSWORD || 'password123';
const PREFIX = __ENV.PREFIX || 'testuser';
const ROOM_PREFIX = __ENV.ROOM_PREFIX || 'loadroom';
const PROFILE = __ENV.PROFILE || 'steady';
const RATE = Number(__ENV.RATE || 100);
const PEAK = Number(__ENV.PEAK || 600);
const DURATION = __ENV.DURATION || '2m';
const WRITE_PCT = Number(__ENV.WRITE_PCT || 10);

const PAGE_SIZE = 50;

// ---------------------------------------------------------------------------
// Metrics
//
// One trend per kind of read, because they are different queries against
// different indexes and Stage 8 moves them to different places. A single
// http_req_duration would average the cheap gap read together with the
// expensive scroll-back and hide exactly the thing being changed.
// ---------------------------------------------------------------------------

const roomList = new Trend('read_room_list', true);
const historyFirst = new Trend('read_history_first_page', true);
const historyScroll = new Trend('read_history_scroll_back', true);
const gapRead = new Trend('read_gap_after_seq', true);
const sendMessage = new Trend('write_send_message', true);
const rateLimited = new Counter('rate_limited_total');

export const options = {
  scenarios: pickScenario(),

  // p50, p95 and p99 are what the README records, and k6 does not print p50 or
  // p99 unless it is told to.
  summaryTrendStats: ['min', 'avg', 'med', 'p(95)', 'p(99)', 'max'],

  thresholds: {
    // No latency thresholds on purpose. This run is measuring what the
    // latency IS; a threshold would be a number invented before the fact.
    // These two are correctness, not performance.
    http_req_failed: ['rate<0.01'],
    checks: ['rate>0.99'],

    // A 429 means the test out-ran the per-user limit and is now measuring the
    // limiter. That is not a slow server, it is a broken test, so it fails
    // loudly rather than quietly skewing the percentiles.
    rate_limited_total: ['count==0'],
  },
};

function pickScenario() {
  if (PROFILE === 'ramp') {
    // An open model: k6 keeps offering this many requests per second whether
    // or not the server keeps up. A closed model (N virtual users in a loop)
    // slows itself down when the server slows down, which is exactly how a
    // saturation point stays hidden.
    return {
      ramp: {
        executor: 'ramping-arrival-rate',
        startRate: 50,
        timeUnit: '1s',
        preAllocatedVUs: 100,
        maxVUs: 1000,
        stages: [
          { target: Math.round(PEAK * 0.25), duration: '30s' },
          { target: Math.round(PEAK * 0.5), duration: '30s' },
          { target: PEAK, duration: '1m' },
          { target: PEAK, duration: '30s' },
        ],
      },
    };
  }

  if (PROFILE !== 'steady') {
    fail(`unknown PROFILE ${PROFILE}; use "steady" or "ramp"`);
  }

  // The default. A fixed offered rate for a fixed time is what makes two runs
  // comparable, and comparing two runs is the entire job of this file.
  return {
    steady: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: 50,
      maxVUs: 500,
    },
  };
}

// ---------------------------------------------------------------------------
// setup: log in, and find the rooms
// ---------------------------------------------------------------------------

export function setup() {
  if (USERS < 1) {
    fail('USERS must be at least 1');
  }
  if (RATE > USERS * 20) {
    console.warn(
      `RATE ${RATE} is above USERS * 20 (${USERS * 20}), the per-user API limit. ` +
        `Expect 429s. Raise USERS, or RATE_LIMIT_API_RPS on the api services.`,
    );
  }

  const users = [];
  for (let i = 1; i <= USERS; i++) {
    const username = `${PREFIX}${String(i).padStart(3, '0')}`;
    const token = login(username);

    const res = http.get(`${BASE_URL}/conversations`, {
      headers: authHeaders(token),
      tags: { name: 'setup_conversations' },
    });
    if (res.status !== 200) {
      fail(`${username}: GET /conversations returned ${res.status}: ${res.body}`);
    }

    // Only the seeded rooms. A database that has been developed against is
    // full of two-message rooms left behind by gapcheck and tracecheck, and
    // those have no second page to scroll back to — they would quietly turn
    // scroll-back iterations into first-page iterations and make two runs
    // incomparable for a reason nothing in the output would show.
    const rooms = res
      .json('conversations')
      .filter((c) => c.title.startsWith(ROOM_PREFIX))
      .map((c) => c.id);
    if (rooms.length === 0) {
      continue;
    }
    users.push({ username, token, rooms });
  }

  if (users.length === 0) {
    fail(
      'no seeded user is in any room, so there is no history to read. ' +
        'Run: make seed ARGS="-n 50 -rooms 20 -messages 200"',
    );
  }

  console.log(`logged in ${users.length} users with rooms, out of ${USERS}`);
  return { users };
}

// login returns an access token, waiting out the auth rate limit rather than
// failing on it. Every login in this script comes from one IP, and the auth
// limiter keys on IP at 5/s — so a 429 here is expected and is not an error.
function login(username) {
  for (let attempt = 0; attempt < 10; attempt++) {
    const res = http.post(
      `${BASE_URL}/login`,
      JSON.stringify({ username, password: PASSWORD }),
      { headers: { 'Content-Type': 'application/json' }, tags: { name: 'setup_login' } },
    );

    if (res.status === 200) {
      return res.json('token');
    }
    if (res.status === 429) {
      const after = Number(res.headers['Retry-After'] || 1);
      sleep(after > 0 ? after : 1);
      continue;
    }
    fail(`login ${username} returned ${res.status}: ${res.body}`);
  }
  fail(`login ${username} kept being rate limited after 10 tries`);
}

// ---------------------------------------------------------------------------
// The iteration
// ---------------------------------------------------------------------------

export default function (data) {
  // A user is picked per iteration rather than per VU. The API rate limit is
  // per user id, so pinning one user to one VU would concentrate the load on
  // whichever users the busiest VUs happened to get.
  const user = data.users[Math.floor(Math.random() * data.users.length)];
  const room = user.rooms[Math.floor(Math.random() * user.rooms.length)];

  const roll = Math.random() * 100;

  if (roll < WRITE_PCT) {
    send(user, room);
  } else if (roll < WRITE_PCT + 10) {
    listRooms(user);
  } else if (roll < WRITE_PCT + 15) {
    readGap(user, room);
  } else if (roll < WRITE_PCT + 40) {
    scrollBack(user, room);
  } else {
    readFirstPage(user, room);
  }
}

// readFirstPage is opening a room: the newest page, no cursor. This is the most
// common read in the product, and the one Stage 8 will put a cache in front of.
function readFirstPage(user, room) {
  const res = get(user, `/conversations/${room}/messages?limit=${PAGE_SIZE}`, 'history_first');
  if (!ok(res, 'first page')) {
    return;
  }
  historyFirst.add(res.timings.duration);
}

// scrollBack is walking backwards through history with ?before_id=. It is the
// read with no index behind it today — (conversation_id) alone, then a sort —
// so it is the one most likely to move when migration 00007 lands.
function scrollBack(user, room) {
  const first = get(user, `/conversations/${room}/messages?limit=${PAGE_SIZE}`, 'history_first');
  if (!ok(first, 'first page before scroll')) {
    return;
  }
  historyFirst.add(first.timings.duration);

  const cursor = first.json('next_before_id');
  if (!cursor) {
    return; // The room is shorter than one page. Nothing to scroll.
  }

  const res = get(
    user,
    `/conversations/${room}/messages?before_id=${cursor}&limit=${PAGE_SIZE}`,
    'history_scroll',
  );
  if (!ok(res, 'scroll back')) {
    return;
  }
  historyScroll.add(res.timings.duration);
}

// readGap is the reconnect repair path from Stage 4. It rides
// idx_msg_seq (conversation_id, seq), so it should already be the fastest read
// here — and if it is not, that is worth knowing before anything is changed.
function readGap(user, room) {
  const from = Math.floor(Math.random() * 150);
  const res = get(
    user,
    `/conversations/${room}/messages?after_seq=${from}&limit=${PAGE_SIZE}`,
    'history_gap',
  );
  if (!ok(res, 'gap read')) {
    return;
  }
  gapRead.add(res.timings.duration);
}

// listRooms is the app opening: every room plus its unread badge.
function listRooms(user) {
  const res = get(user, '/conversations', 'room_list');
  if (!ok(res, 'room list')) {
    return;
  }
  roomList.add(res.timings.duration);
}

// send keeps writes in the mix, because the whole premise of Stage 8 is that
// reads and writes compete for the same 25 connections. A pure read test would
// measure a pool nobody else is using.
function send(user, room) {
  const body = JSON.stringify({
    content: `k6 ${exec.scenario.iterationInTest} from ${user.username}`,
  });
  const res = http.post(`${BASE_URL}/conversations/${room}/messages`, body, {
    headers: { ...authHeaders(user.token), 'Content-Type': 'application/json' },
    tags: { name: 'send' },
  });
  if (!ok(res, 'send', 201)) {
    return;
  }
  sendMessage.add(res.timings.duration);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function get(user, path, name) {
  return http.get(`${BASE_URL}${path}`, {
    headers: authHeaders(user.token),
    tags: { name },
  });
}

function authHeaders(token) {
  return { Authorization: `Bearer ${token}` };
}

// ok records the check and counts a 429 separately, because a rate-limited
// request is a broken test rather than a slow server and the two must not be
// added together.
function ok(res, what, want = 200) {
  if (res.status === 429) {
    rateLimited.add(1);
  }
  return check(res, { [`${what} is ${want}`]: (r) => r.status === want });
}

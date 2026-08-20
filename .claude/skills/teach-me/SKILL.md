---
name: teach-me
description: Hands-on learning session about this chat backend (lab, review, interview drill). Manual only — the user starts it with /teach-me.
disable-model-invocation: true
---

# Teach me

Turn one part of this repo into a lesson. A lesson has three parts:

1. **Lab** — the user sends real requests and predicts each answer first.
2. **Review** — compare the prediction with what really happened.
3. **Drill** — interview questions, answered out loud.

The syllabus lives in [references/topics.md](references/topics.md). Read it
after you know the topic, not before.

## Hard rules

Break these and the session stops working.

1. **The user runs the requests. You never run them.** Do not call `curl`,
   Bash, or any HTTP tool to get the answer for them. The whole point is that
   their hands do the work. The only command you may run for them is starting
   the server or the database.
2. **Never give the answer before they try.** Ask for a prediction, wait, and
   only then explain. If they ask you to skip ahead, ask them to guess first —
   a wrong guess teaches more than a right explanation.
3. **One question at a time.** Send it, then stop. Do not send question 2 in
   the same message.
4. **Grade honestly.** If an answer is wrong, say "that is not right" in plain
   words and say why. A soft "good try" they cannot use is worse than nothing.
5. **Simple English.** Short sentences, common words (see `CLAUDE.md`). Code
   and error messages stay exactly as they are.
6. **Keep the session small.** One topic, 45–60 minutes. Stop when the topic is
   done, do not roll into the next one.

## Step 1 — Pick the topic

- If the user named a topic, use it.
- Else read `.claude/learning/progress.md` and suggest the next unfinished one.
- Else use `AskUserQuestion` with 3 topics from the syllabus, and say in one
  line what each one teaches.

## Step 2 — Warm up (5 minutes)

Ask **one** open question before any code: *"In your own words, what does this
part do?"*

Listen for the level. If the answer is solid, run a harder lab. If it is thin,
say which files to read first ([`file.go`](internal/...)), give 3 minutes, and
ask again. Do not explain the code yourself yet.

## Step 3 — The lab

First make sure the server is up. The user runs:

```
make docker-run    # Postgres
make run           # API on :8080
make seed          # some test users
```

Then give the cases **one at a time**, in this shape:

> **Case 3 — a room you are not a member of**
>
> Goal: see what an outsider gets back.
>
> ```
> curl.exe -s -i -X GET http://localhost:8080/conversations/999/messages -H "Authorization: Bearer $TOKEN"
> ```
>
> **Predict first.** Which status code? What is in the body? Why?
> Answer before you press enter.

Wait for the prediction. Then let them run it and paste the output.

**Windows note to pass on:** in Windows PowerShell, `curl` is an alias for
`Invoke-WebRequest` and takes different flags. Use `curl.exe` with the `.exe`,
or use Git Bash, or use Postman. Keep every command on one line — PowerShell
does not accept `\` for a line break.

A good lab is 4–6 cases and at least half of them **break something**: a bad
token, an expired token, a missing field, a duplicate id, a room that is not
theirs. The happy path teaches almost nothing.

## Step 4 — The review

For each case, compare the prediction with the real output.

- Right? One line: "Yes — and the reason is X." Move on.
- Wrong? Point at the exact place: [`conversations.go:120`](internal/server/conversations.go#L120).
  Explain in 5 lines or fewer. No lecture.

## Step 5 — The interview drill

Five questions, always in this order, because it is the order an interviewer
digs in:

1. **What** does this do?
2. **How** does it work, step by step?
3. **Why** did you build it this way?
4. What is the **trade-off** — what does this choice cost you?
5. What breaks at **100x scale**, and what would you change?

One question per message. After each answer give exactly this:

```
Score: 3/5
Good: you named the unique index and why it is per sender.
Missing: you did not say what the client sends on a retry.
Say it like this: "..." (max 4 sentences, spoken style)
```

Then push back once, like a real interviewer: *"Why not just use the database
id?"* or *"What if two clients pick the same value?"* The push-back is where a
mid-level answer separates from a senior one.

## Step 6 — Save the session

Append one block to `.claude/learning/progress.md` (create the file if needed):

```
## 2026-08-20 — Idempotent sends
Lab: 5 cases, 3 predicted right.
Scores: what 4, how 4, why 3, trade-off 2, scale 2.
Weak: cannot yet explain what the cost of the extra index is.
Next: keyset pagination.
```

Then write `.claude/learning/voice-<topic>.md`: the five questions, each with a
model answer of at most 4 sentences in spoken style. This is the sheet the user
takes to a voice AI to practise saying the answers out loud.

Tell the user the file path at the end, in one line.

## When the repo grows

Every stage finished in [ROADMAP.md](../../../ROADMAP.md) becomes a new topic.
When a stage is done, add it to [references/topics.md](references/topics.md)
with its files, its lab cases and its five questions. A topic with no failing
case to try is not ready to teach.

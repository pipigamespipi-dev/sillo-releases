# sillo-sync relay

A tiny Flask pass-through mailbox between the Sillo desktop app and the phone PWA.
The desktop generates the pairing token; the relay just queues items per token+direction
(and holds a few photos in memory). It never generates tokens, has no accounts, and
nothing is durable — queues get a best-effort `data.json` dump, photos live in RAM only.

**Deploy on Render:** push this folder to a repo → Render dashboard → *New + → Web Service* →
pick the repo → Render reads `render.yaml` (free plan, `gunicorn app:app`). Done.

**Privacy:** pure pass-through. No logs of tokens or content (method+path+status only),
no analytics, everything evaporates on restart. Test locally with `python selftest.py`.

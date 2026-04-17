# L.A.K.E. Testing Notes

## Smoke Tests

Run:

```bash
python -m unittest discover -s tests -v
```

## What The Tests Cover

- All tracked Python files compile successfully.
- Release-critical sections exist in `README.md`.
- The local documentation scaffold exists.

## What The Tests Do Not Cover

- End-to-end execution against a live Blue service.
- Real database connectivity.
- OpenAI-backed planner behavior.

Those checks still require a configured Blue environment, valid credentials, and a reachable service URL.

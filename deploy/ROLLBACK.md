# Rollback

Use these scripts:

1. Deploy current commit and record release id:
```bash
./scripts/deploy_release.sh deploy/.env.live
```

2. Roll back to previously deployed release:
```bash
./scripts/rollback_release.sh deploy/.env.live
```

State files:
- `.deploy/current_release`
- `.deploy/previous_release`

If scripts are unavailable, manual rollback:
```bash
git checkout <previous_commit_or_tag>
docker compose --env-file deploy/.env.live up --build -d
curl -fsS http://localhost:8000/health
```

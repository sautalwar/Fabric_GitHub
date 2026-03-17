## 📝 What does this PR do?

<!-- Brief description of the changes -->

## 🗄️ Migration Changes

<!-- If this PR includes migration files, describe the schema changes -->

- [ ] New tables created
- [ ] Columns added/modified
- [ ] Indexes updated
- [ ] No migration changes

## ✅ Checklist

- [ ] Migration files follow `vNNN_description.sql` naming convention
- [ ] SQL validated locally
- [ ] No breaking changes to existing tables
- [ ] Documentation updated (if applicable)

## 🚀 Deployment Path

```
PR Merge → Validate → Deploy UAT → ⏸️ Approval → Deploy Prod
```

## 🧪 Testing

<!-- How was this tested? -->

- [ ] Ran `python scripts/validate_migrations.py` locally
- [ ] Tested in Dev workspace

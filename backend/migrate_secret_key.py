"""One-time migration: re-encrypt all datasource credentials after SECRET_KEY change.

Usage:
    OLD_SECRET_KEY=<old-key> ./venv/bin/python migrate_secret_key.py

The script reads the OLD key from the OLD_SECRET_KEY env var and the NEW key
from the current .env / SECRET_KEY setting.  It decrypts each datasource's
connection_encrypted with the old key, then re-encrypts with the new key.
"""
import os
import sys
import base64
import hashlib
from cryptography.fernet import Fernet, InvalidToken

# Bootstrap app settings (reads current .env)
sys.path.insert(0, os.path.dirname(__file__))
from app.config import settings
from app.models import SessionLocal, Datasource

OLD_KEY = os.environ.get("OLD_SECRET_KEY", "").strip()
NEW_KEY = settings.secret_key

if not OLD_KEY:
    print("ERROR: Set OLD_SECRET_KEY env var to your previous secret key.")
    print("  Example: OLD_SECRET_KEY=BayanSecretKey ./venv/bin/python migrate_secret_key.py")
    sys.exit(1)

if OLD_KEY == NEW_KEY:
    print("ERROR: OLD_SECRET_KEY and current SECRET_KEY are the same. Nothing to migrate.")
    sys.exit(1)


def _derive(secret: str) -> bytes:
    return base64.urlsafe_b64encode(hashlib.sha256(secret.encode()).digest())


old_fernet = Fernet(_derive(OLD_KEY))
new_fernet = Fernet(_derive(NEW_KEY))

db = SessionLocal()
try:
    datasources = db.query(Datasource).all()
    migrated = 0
    failed = 0
    for ds in datasources:
        enc = getattr(ds, "connection_encrypted", None)
        if not enc:
            continue
        try:
            plain = old_fernet.decrypt(enc.encode("utf-8")).decode("utf-8")
            ds.connection_encrypted = new_fernet.encrypt(plain.encode("utf-8")).decode("utf-8")
            db.add(ds)
            migrated += 1
            print(f"  OK: {ds.id} ({getattr(ds, 'name', '?')})")
        except InvalidToken:
            failed += 1
            print(f"  SKIP: {ds.id} — could not decrypt (already migrated or different key)")
        except Exception as e:
            failed += 1
            print(f"  ERROR: {ds.id} — {e}")

    # Also re-encrypt email config password if present
    try:
        from app.models import EmailConfig
        for ec in db.query(EmailConfig).all():
            enc = getattr(ec, "password_encrypted", None)
            if not enc:
                continue
            try:
                plain = old_fernet.decrypt(enc.encode("utf-8")).decode("utf-8")
                ec.password_encrypted = new_fernet.encrypt(plain.encode("utf-8")).decode("utf-8")
                db.add(ec)
                migrated += 1
                print(f"  OK: EmailConfig {ec.id}")
            except InvalidToken:
                print(f"  SKIP: EmailConfig {ec.id} — could not decrypt")
            except Exception as e:
                print(f"  ERROR: EmailConfig {ec.id} — {e}")
    except Exception:
        pass

    if migrated > 0:
        db.commit()
        print(f"\nDone: {migrated} credentials re-encrypted, {failed} skipped.")
    else:
        print(f"\nNo credentials to migrate ({failed} skipped).")
finally:
    db.close()

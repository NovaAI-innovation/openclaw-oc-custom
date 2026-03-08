#!/bin/bash
set -e
KEY="$1"
BUCKET="openclaw"
ACCESS_KEY="972d38bf8ca554615f49ed091b1b8851"
SECRET_KEY="93136fb2f17da2aaa032463a48302ec983f64f5581f4773701560ee039de38a1"
ACCOUNT_ID="554cefcc58bccd23ab124a9c06582d06"
python3 ../r2_integration/cloudflare_r2.py download_brief "$KEY" "$BUCKET" "$ACCESS_KEY" "$SECRET_KEY" "$ACCOUNT_ID"

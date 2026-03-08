#!/bin/bash
slug=\$1
python3 /root/cloudflare_r2.py download_brief "orgs/org_agency/global/capabilities/capability-catalog.json" openclaw 972d38bf8ca554615f49ed091b1b8851 93136fb2f17da2aaa032463a48302ec983f64f5581f4773701560ee039de38a1 554cefcc58bccd23ab124a9c06582d06 > /tmp/cap_catalog.json
jq -r ".capabilities[] | select(.slug == \"\$slug\") | .domain" /tmp/cap_catalog.json
rm /tmp/cap_catalog.json

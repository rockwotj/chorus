#!/usr/bin/env python3
"""Audit or remove only this run's objects and empty Rapid folders.

Uses the invoking gcloud account. Tokens stay in memory and are never logged.
Deletion is generation/metageneration guarded. No bucket/IAM changes.
"""
import argparse
import json
import subprocess
import urllib.parse
import urllib.request

ROOT = "chorus-experiments/slatedb-readers-20260904-172428/"
BUCKETS = [f"subspace-dev-rapid-zonal-{n}" for n in range(1, 4)] + [
    "subspace-dev-regional"
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--delete", action="store_true")
    args = parser.parse_args()
    token = subprocess.check_output(
        ["gcloud", "auth", "print-access-token"], text=True
    ).strip()

    def request(bucket, suffix, query=None, method="GET"):
        url = "https://storage.googleapis.com/storage/v1/b/" + bucket + "/" + suffix
        if query:
            url += "?" + urllib.parse.urlencode(query)
        req = urllib.request.Request(
            url, headers={"Authorization": "Bearer " + token}, method=method
        )
        with urllib.request.urlopen(req, timeout=60) as response:
            data = response.read()
            return json.loads(data) if data else {}

    def listing(bucket, resource):
        query = {"prefix": ROOT}
        if resource == "o":
            query["versions"] = "true"
        out = []
        while True:
            page = request(bucket, resource, query)
            assert page.get("kind") == ("storage#objects" if resource == "o" else "storage#folders")
            out.extend(page.get("items", []))
            if not page.get("nextPageToken"):
                return out
            query["pageToken"] = page["nextPageToken"]

    inventories = []
    for bucket in BUCKETS:
        objects = listing(bucket, "o")
        folders = listing(bucket, "folders") if "rapid" in bucket else []
        for obj in objects:
            assert obj["name"].startswith(ROOT)
            assert int(obj["generation"]) > 0
        for folder in folders:
            assert folder["name"].startswith(ROOT) and folder["name"].endswith("/")
            assert int(folder["metageneration"]) > 0
        inventories.append((bucket, objects, folders))
        print(json.dumps({"event": "inventory", "bucket": bucket, "root": ROOT,
                          "objects": objects, "folders": folders}), flush=True)
    if not args.delete:
        return
    for bucket, objects, folders in inventories:
        for obj in objects:
            request(bucket, "o/" + urllib.parse.quote(obj["name"], safe=""),
                    {"generation": obj["generation"], "ifGenerationMatch": obj["generation"]},
                    method="DELETE")
        assert not listing(bucket, "o"), "objects remain; refusing folder removal"
        for folder in sorted(folders, key=lambda f: len(f["name"]), reverse=True):
            request(bucket, "folders/" + urllib.parse.quote(folder["name"], safe=""),
                    {"ifMetagenerationMatch": folder["metageneration"]}, method="DELETE")
        assert not listing(bucket, "o")
        assert "rapid" not in bucket or not listing(bucket, "folders")
        print(json.dumps({"event": "clean", "bucket": bucket, "root": ROOT,
                          "deleted_objects": len(objects), "deleted_folders": len(folders)}), flush=True)


if __name__ == "__main__":
    main()

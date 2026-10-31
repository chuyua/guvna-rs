#!/usr/bin/env python3
"""Official SDK clients against the real gateway router and a local native mock.

No external API key or paid service. Run via the ignored Rust HTTP fixture.
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys

from openai import OpenAI, DefaultHttpxClient, NotFoundError
import openai


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--node-package", required=True)
    parser.add_argument("--out", default="sdk-results.json")
    args = parser.parse_args()
    result = {"passed": False, "python_version": openai.__version__}
    try:
        with OpenAI(base_url=args.base_url, api_key="client", max_retries=0,
                    http_client=DefaultHttpxClient(trust_env=False)) as client:
            created = client.responses.create(model="demo", input="sdk", background=True)
            assert created.status == "queued"
            assert client.responses.retrieve(created.id).model == "demo"
            page = client.responses.input_items.list(created.id, limit=1, order="asc")
            assert page.data[0].id == "item_1" and not page.has_more
            assert client.responses.cancel(created.id).status == "cancelled"
            continuation = client.responses.create(model="demo", input="next", previous_response_id=created.id)
            assert continuation.status == "completed" and continuation.output_text == "hello"
            assert client.responses.input_tokens.count(model="demo", input="sdk").input_tokens == 17
            with client.responses.create(model="demo", input="sdk", stream=True, store=False) as stream:
                events = list(stream)
            assert events[0].type == "response.created" and events[-1].type == "response.completed"
            assert "".join(e.delta for e in events if e.type == "response.output_text.delta") == "hello"
            chat = client.chat.completions.create(model="demo", messages=[{"role":"user","content":"sdk"}])
            assert chat.choices[0].message.content == "hello"
            chunks = list(client.chat.completions.create(model="demo", messages=[{"role":"user","content":"sdk"}], stream=True))
            assert "".join(c.choices[0].delta.content or "" for c in chunks) == "hello"
            assert chunks[-1].choices[0].finish_reason == "stop"
            client.responses.delete(created.id)
            try:
                client.responses.retrieve(created.id)
                raise AssertionError("deleted response was returned")
            except NotFoundError as error:
                assert error.status_code == 404 and error.code == "response_not_found"
        output = subprocess.run(["node", str(Path(__file__).with_name("sdk_smoke.cjs")),
                                 args.base_url, args.node_package], check=True, capture_output=True, text=True)
        result["node"] = json.loads(output.stdout)
        result["passed"] = True
    finally:
        Path(args.out).write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result))


if __name__ == "__main__":
    main()

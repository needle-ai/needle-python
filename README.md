# Needle Python Library

[![PyPI - Version](https://img.shields.io/pypi/v/needle-python.svg)](https://pypi.org/project/needle-python)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/needle-python.svg)](https://pypi.org/project/needle-python)

This Python library provides convenient access to Needle API. There are various methods and data types that we believe will help you explore Needle API quickly. There may be some functionality available in REST API earlier than this Python library. In any case, we recommend to take a look at the complete [documentation](https://docs.needle.app). Thank you for flying with us. 🚀

## Installation

This library requires Python >3.8 and `pip` to use. You don't need the sources unless you want to modify it. Install with:

```
pip install needle-python
```

## Usage ⚡️

To get started, generate an API key for your account in the developer settings menu at [Needle](https://needle.app). Note that your key will be valid until you revoke it. Set the following env variable before you run your code:

```
export NEEDLE_API_KEY=<your-api-key>
```

`NeedleClient` reads the API key from the environment by default. If you would like to override this behaviour you can pass it in as a parameter. 

### Retrieve context from Needle

```python
from needle.v1 import NeedleClient
from needle.v1.models import FileToAdd


ndl = NeedleClient()
collection = ndl.collections.create(name="Tech Trends")

# add file to collection
files = ndl.collections.files.add(
    collection_id=collection_id,
    files=[
        FileToAdd(
            name="tech-radar-30.pdf",
            url="https://www.thoughtworks.com/content/dam/thoughtworks/documents/radar/2024/04/tr_technology_radar_vol_30_en.pdf",
        )
    ],
)

# wait until indexing is complete
files = ndl.collections.files.list(collection_id)
if not all(f.status == "indexed" for f in files):
    time.sleep(5)
    files = ndl.collections.files.list(collection_id)

# retrieve relevant context
prompt = "What techniques moved into adopt in this volume of technology radar?"
results = ndl.collections.search(collection_id, text=prompt)
```

Needle instantly extracts key points from your files.

### Complete your RAG pipeline

Naturally, to compose a human-friendly answer use an LLM provider of your choice. For demo purposes, we used OpenAI in this example:

```python
from openai import OpenAI

system_messages = [{"role": "system", "content": r.content} for r in results] # results from Needle
user_message = {
    "role": "user",
    "content": f"""
        Only answer the question based on the provided results data. 
        If there is no data in the provided data for the question, do not try to generate an answer.
        This is the question: {prompt}
""",
}

openai_client = OpenAI()
answer = openai_client.chat.completions.create(
    model="gpt-3.5-turbo",
    messages=[
        *system_messages,
        user_message,
    ],
)

print(answer.choices[0].message.content)
# -> Retrieval-Augmented Generation (RAG) is the technique that moved into "Adopt" in this volume of the Technology Radar.
```

This is one basic example of a RAG pipeline you can quickly implement using Needle and OpenAI. Feel free to engineer more precise prompts and explore other prompting techniques such as chain-of-thought (CoT), graph of thoughts (GoT) etc. 

Needle API helps you with hassle-free contextualization however it does not limit you to a certain RAG technique. Let us know what you build in our [Discord channel](https://discord.gg/JzJcHgTyZx) :)

## Exceptions 🧨

If a request to Needle API fails, `needle.v1.models.Error` object will be raised. There you can see a `message` and more details about the error.

## Support 📞

If you have questions, you can contact us in our [Discord channel](https://discord.gg/JzJcHgTyZx). 

# License

`needle-python` is distributed under the terms of the MIT license.

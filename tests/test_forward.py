import base64
import json

import pytest

import log_forwarder.clients
from log_forwarder.forward import process


EVENT = {'awslogs': {'data': 'H4sIAAAAAAAA/7VWXW/bVhL9K8LFPkryfN6Zyzc3dYMAdlLU7hbdKDAo6cohIJNakrLrBv7vC1IO0GAtNAJ238iZuXMuzxnOzJdwn7uuvMs3T7scivDj+c357dXF9fX524swDc1jndtQBGWmaJAc0cM0bJu7t22z34UinJWP3dm2vF+uy7OqvstdXzX1VVNXfdP+tK9Xw+vhxHXf5vI+FIGA9AzhDPjs4z8uz28urm8+Uam43JSicYOiVC6TlymhS1nKRgTDNHT7Zbdqq92Q8adq2+e2C8XHb8yzzWgPn0bAi4dc90PMl1CtQxE4ETsnTWIRk6VEkQlZ2IUlEjmJkKgkZFSV6MLsaDFMQ1/d564v73ehQNOkCEombtOv9IUifFmMYYtQLMZPnCHMgG8wFqoF2Vzc/rUI00Xon3aHqN227DdNez/v+rLtR1+bV027XoTiy/D8733u+nfrMdhj3HCJebZmX81kDetZWTLPCMiWOUsE1DHF5oX187YeD5ZtXZSPXXEQqcj72WPu+hkWfxW1+HqqOCbimPsht934XCzCi3SL8Pwcnqf/zbEpR2BjERjYRWMxAnZjQVcgNGIjIIJofJTjxK9wPIYtQjF5lemB5ckibPND3h6Cfjv/5f27928P9pdsB8/FH7um7XM7KbdtLtdPk+7zvl83j/V0Ut3VTVvVd5Nl2a8+v+Rs7u5yezja7HLd522+z337NM8vieZNv93Nd23TN/PPfb+bD+5qdfvVf8jzV2kni5DWsFxGW84iLnEmsIFZEsCZs6xWKfoy+mYRnhf1a0Sn5BzNSYXUoou4RdKIoBFFkqu5CaNTJDI5RrT+n4n+8JDbtloPfDabyWrftrnuJ1e5z+3PbfNQrXM7qbpJ3fSTcrttHvP67xg/ENvNb6u6z21dbl+l9vt+m9epVSJkUzITRDCJDmiW0IndDSUmAZKEkYey1mPUxm+p/SV3u6bucjEhgMnrwKyKlkw8ApG6YAIyZFJxBWNNajiIhuTORxuU6cnA4p5caGiPLEI4FE9kia6OwiTJBDQyJ2VLdgzY8WTgoTjdY4LkEkk9RSPViBhtoJxTgqQpkZmb+lFgOxnYNGpijISmDimyqg3NSwbVFRjAPLLi0MswHQNOfDKwJ+BkkZljTInZRIiTJ3MDhURAYtEQhDw6HAU+ubgikBAQgqg6miEyGRMIpBg9OoJgNIiJlOM4eV8Ddji5uCKqA0Q1Hdp6BFdCIkEXiExkAOpqQj4wDnQMGE8urkgJHRHGH1ZFNEEyjEyGgw6aYJR7qD7WeGwWOZ5cXFGGRqyKjoxGzpScwMWAVJOIwxCJokaudKw3O51cXFGMRAycQTwKRXZBh2H+cooGQuKUzMZrxaMak8r3bzg+J5VjG06bh/H3v1pxXlr/Icd635bjvjK8W4xzAZ0uwrLabvP6x2+dNh6+b9qn6+rPfPXDIhRIPhjLP65G+69dXo920+fpInR92e+78XbdfrXKXXfYdz5Nw67ZVquny2H4hSKcv3nz4df3N7eXF/+8uLz9+cPluze/h+f/AK22qw1fCwAA'}}
MESSAGES = [
    "{\"time\":\"2025-10-03T16:55:27.487Z\",\"type\":\"platform.start\",\"record\":{\"requestId\":\"866f3a1e-d38c-4d0d-aa33-2027bee46015\",\"functionArn\":\"arn:aws:lambda:eu-west-1:533267098118:function:ingestionMonitorFunction\",\"version\":\"$LATEST\"}}",
    "{\"timestamp\": \"2025-10-03T16:55:27Z\", \"level\": \"WARNING\", \"message\": \"Exporter already shutdown, ignoring batch\", \"logger\": \"opentelemetry.exporter.otlp.proto.http.metric_exporter\", \"requestId\": \"9d0bb67b-61b1-40f0-9401-834cc968b68f\"}",
    "{\"timestamp\": \"2025-10-03T16:55:27Z\", \"level\": \"WARNING\", \"message\": \"Overriding of current MeterProvider is not allowed\", \"logger\": \"opentelemetry.metrics._internal\", \"requestId\": \"866f3a1e-d38c-4d0d-aa33-2027bee46015\"}",
    "Response: 200",
    "Response: 200",
    "Response: 200",
    "Response: 200",
    "Response: 200",
    "Response: 200",
    "Response: 200",
    "Response: 200",
    "Response: 200",
    "Response: 200",
    "{\"time\":\"2025-10-03T16:55:28.254Z\",\"type\":\"platform.report\",\"record\":{\"requestId\":\"866f3a1e-d38c-4d0d-aa33-2027bee46015\",\"metrics\":{\"durationMs\":766.405,\"billedDurationMs\":767,\"memorySizeMB\":128,\"maxMemoryUsedMB\":75},\"status\":\"success\"}}"
]


def test_forward(monkeypatch):
    log_data = []
    destination_config = {'/aws/lambda/ingestionMonitorFunction': {'tags': 'my_tag_key,my_tag_value'}}
    destination_config_str = json.dumps(destination_config)
    monkeypatch.setenv('destination_config', base64.b64encode(destination_config_str.encode()).decode())
    monkeypatch.setattr(log_forwarder.forward.BrontoClient, 'send_data',
                        lambda _, batch, attributes=None: log_data.extend(batch.batch))
    process(EVENT)
    assert log_data == MESSAGES


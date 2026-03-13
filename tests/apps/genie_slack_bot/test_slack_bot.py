import importlib
import sys
import threading
import types
from pathlib import Path

APP_DIR = Path(__file__).resolve().parents[3] / "apps" / "genie-slack-bot"


def load_slack_bot_module():
    databricks_genie_client = types.ModuleType("databricks_genie_client")
    databricks_genie_client.DatabricksGenieClient = object
    sys.modules["databricks_genie_client"] = databricks_genie_client

    slack_bolt = types.ModuleType("slack_bolt")

    class FakeApp:
        def __init__(self, *args, **kwargs):
            self.handlers = []

        def event(self, name):
            def decorator(func):
                self.handlers.append(("event", name, func))
                return func

            return decorator

        def action(self, name):
            def decorator(func):
                self.handlers.append(("action", name, func))
                return func

            return decorator

    slack_bolt.App = FakeApp
    sys.modules["slack_bolt"] = slack_bolt

    socket_mode = types.ModuleType("slack_bolt.adapter.socket_mode")

    class FakeSocketModeHandler:
        def __init__(self, *args, **kwargs):
            pass

        def start(self):
            pass

    socket_mode.SocketModeHandler = FakeSocketModeHandler
    sys.modules["slack_bolt.adapter"] = types.ModuleType("slack_bolt.adapter")
    sys.modules["slack_bolt.adapter.socket_mode"] = socket_mode

    slack_sdk = types.ModuleType("slack_sdk")

    class FakeWebClient:
        def __init__(self, *args, **kwargs):
            pass

    slack_sdk.WebClient = FakeWebClient
    sys.modules["slack_sdk"] = slack_sdk

    sys.path.insert(0, str(APP_DIR))
    sys.modules.pop("slack_bot", None)
    return importlib.import_module("slack_bot")


class FakeGenieClient:
    """Test double for the Genie client."""

    def __init__(self):
        """Initialize captured call state."""
        self.calls = []
        self.counter = 0
        self.feedback_calls = []
        self.feedback_success = True

    def ask_question(self, text, conversation_id=None, on_message_sent=None):
        """Return a deterministic Genie response for tests."""
        self.calls.append((text, conversation_id))
        self.counter += 1
        conversation_id = conversation_id or f"conv-{self.counter}"
        if on_message_sent is not None:
            on_message_sent(conversation_id, f"msg-{self.counter}")
        return {
            "success": True,
            "conversation_id": conversation_id,
            "message_id": f"msg-{self.counter}",
            "response": f"response-{self.counter}",
            "result_data": None,
            "suggested_questions": [],
        }

    def send_message_feedback(self, **kwargs):
        """Pretend feedback submission succeeded."""
        self.feedback_calls.append(kwargs)
        return self.feedback_success


class FakeSlackClient:
    """Test double for the Slack WebClient."""

    def __init__(self):
        """Initialize captured Slack messages."""
        self.post_count = 0
        self.posts = []
        self.updates = []

    def chat_postMessage(self, **payload):
        """Capture posted messages and return a synthetic timestamp."""
        self.post_count += 1
        ts = str(1000 + self.post_count)
        message = {**payload, "ts": ts}
        self.posts.append(message)
        return message

    def chat_update(self, **payload):
        """Capture message updates and echo the payload."""
        self.updates.append(payload)
        return payload


def build_bot():
    slack_bot = load_slack_bot_module()
    genie_client = FakeGenieClient()
    bot = slack_bot.SlackGenieBot(
        "xoxb-test",
        "secret",
        "xapp-test",
        genie_client,
    )
    return bot, genie_client


def test_duplicate_thread_reply_event_is_processed_once():
    bot, genie_client = build_bot()
    client = FakeSlackClient()

    root_event = {
        "channel": "C123",
        "channel_type": "channel",
        "ts": "150.100",
        "text": "<@U123BOT> How many elections do we have?",
    }
    reply_event = {
        "channel": "C123",
        "channel_type": "channel",
        "ts": "150.200",
        "thread_ts": "150.100",
        "text": "<@U123BOT> How many by year?",
    }

    bot._handle_message(root_event, None, client)
    assert bot._should_handle_message_event(reply_event) is True

    bot._handle_message(reply_event, None, client)
    bot._handle_message(reply_event, None, client)

    assert genie_client.calls == [
        ("How many elections do we have?", None),
        ("How many by year?", "conv-1"),
    ]


def test_dm_message_events_are_always_handled():
    bot, _ = build_bot()

    top_level_dm = {
        "channel": "D123",
        "channel_type": "im",
        "ts": "101.100",
        "text": "How many elections do we have?",
    }
    reply_dm = {
        "channel": "D123",
        "channel_type": "im",
        "ts": "101.200",
        "thread_ts": "101.100",
        "text": "How many by year?",
    }

    assert bot._should_handle_message_event(top_level_dm) is True
    assert bot._should_handle_message_event(reply_dm) is True


def test_message_events_with_subtype_or_bot_id_are_ignored():
    bot, _ = build_bot()

    assert (
        bot._should_handle_message_event(
            {
                "channel": "D123",
                "channel_type": "im",
                "ts": "101.100",
                "text": "hello",
                "subtype": "message_changed",
            }
        )
        is False
    )
    assert (
        bot._should_handle_message_event(
            {
                "channel": "D123",
                "channel_type": "im",
                "ts": "101.101",
                "text": "hello",
                "bot_id": "B123",
            }
        )
        is False
    )


def test_empty_message_prompts_for_question():
    bot, genie_client = build_bot()
    client = FakeSlackClient()

    mention_only_event = {
        "channel": "C123",
        "channel_type": "channel",
        "ts": "175.100",
        "text": "<@U123BOT>",
    }

    bot._handle_message(mention_only_event, None, client)

    assert genie_client.calls == []
    assert client.posts == [
        {
            "channel": "C123",
            "text": "Please ask me a question about your data!",
            "thread_ts": "175.100",
            "ts": "1001",
        }
    ]


def test_dm_top_level_messages_start_new_conversations():
    bot, genie_client = build_bot()
    client = FakeSlackClient()

    first_dm = {
        "channel": "D123",
        "channel_type": "im",
        "ts": "111.111",
        "text": "How many elections do we have?",
    }
    second_dm = {
        "channel": "D123",
        "channel_type": "im",
        "ts": "112.112",
        "text": "How many are in Colorado?",
    }

    bot._handle_message(first_dm, None, client)
    bot._handle_message(second_dm, None, client)

    assert genie_client.calls == [
        ("How many elections do we have?", None),
        ("How many are in Colorado?", None),
    ]


def test_channel_follow_up_reuses_parent_thread_conversation():
    bot, genie_client = build_bot()
    client = FakeSlackClient()

    mention_event = {
        "channel": "C123",
        "channel_type": "channel",
        "ts": "200.100",
        "text": "<@U123BOT> How many elections do we have?",
    }
    reply_event = {
        "channel": "C123",
        "channel_type": "channel",
        "ts": "200.200",
        "thread_ts": "200.100",
        "text": "How many by year?",
    }

    bot._handle_message(mention_event, None, client)

    assert bot._should_handle_message_event(reply_event) is True

    bot._handle_message(reply_event, None, client)

    assert genie_client.calls == [
        ("How many elections do we have?", None),
        ("How many by year?", "conv-1"),
    ]


def test_channel_thread_without_existing_mapping_is_ignored():
    bot, _ = build_bot()

    reply_event = {
        "channel": "C123",
        "channel_type": "channel",
        "ts": "300.200",
        "thread_ts": "300.100",
        "text": "How many by year?",
    }

    assert bot._should_handle_message_event(reply_event) is False


def test_dm_thread_follow_up_reuses_parent_conversation():
    bot, genie_client = build_bot()
    client = FakeSlackClient()

    first_dm = {
        "channel": "D123",
        "channel_type": "im",
        "ts": "111.111",
        "text": "How many elections do we have?",
    }
    reply_dm = {
        "channel": "D123",
        "channel_type": "im",
        "ts": "111.222",
        "thread_ts": "111.111",
        "text": "How many are in Colorado?",
    }

    bot._handle_message(first_dm, None, client)
    bot._handle_message(reply_dm, None, client)

    assert genie_client.calls == [
        ("How many elections do we have?", None),
        ("How many are in Colorado?", "conv-1"),
    ]


def test_feedback_routes_to_genie_and_updates_message():
    bot, genie_client = build_bot()
    client = FakeSlackClient()
    bot._remember_mapping(
        bot.message_feedback_map,
        "500.100",
        ("conv-1", "msg-1"),
        2000,
    )

    bot._handle_feedback(
        {
            "message": {"ts": "500.100"},
            "channel": {"id": "C123"},
            "user": {"id": "U123"},
        },
        "positive",
        client,
    )

    assert genie_client.feedback_calls == [
        {
            "conversation_id": "conv-1",
            "message_id": "msg-1",
            "rating": "positive",
        }
    ]
    assert client.updates == [
        {
            "channel": "C123",
            "ts": "500.100",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "👍 _Thanks for your feedback!_",
                    },
                }
            ],
            "text": "👍 _Thanks for your feedback!_",
        }
    ]


def test_feedback_without_mapping_updates_with_failure_message():
    bot, _ = build_bot()
    client = FakeSlackClient()

    bot._handle_feedback(
        {
            "message": {"ts": "500.200"},
            "channel": {"id": "C123"},
            "user": {"id": "U123"},
        },
        "negative",
        client,
    )

    assert client.updates == [
        {
            "channel": "C123",
            "ts": "500.200",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            "⚠️ _Unable to submit feedback. Please try asking a new "
                            "question._"
                        ),
                    },
                }
            ],
            "text": "Unable to submit feedback",
        }
    ]


def test_genie_error_updates_thinking_message_with_apology():
    slack_bot = load_slack_bot_module()

    class ErroringFakeGenieClient(FakeGenieClient):
        def ask_question(self, text, conversation_id=None, on_message_sent=None):
            raise RuntimeError("boom")

    genie_client = ErroringFakeGenieClient()
    bot = slack_bot.SlackGenieBot(
        "xoxb-test",
        "secret",
        "xapp-test",
        genie_client,
    )
    client = FakeSlackClient()

    bot._handle_message(
        {
            "channel": "D123",
            "channel_type": "im",
            "ts": "600.100",
            "text": "How many elections do we have?",
        },
        None,
        client,
    )

    assert client.posts == [
        {
            "channel": "D123",
            "text": "Thinking...",
            "thread_ts": "600.100",
            "ts": "1001",
        }
    ]
    assert client.updates == [
        {
            "channel": "D123",
            "ts": "1001",
            "text": (
                "Sorry, I encountered an error processing your request. "
                "Please try again."
            ),
        }
    ]


def test_fast_thread_follow_up_reuses_existing_conversation_scope():
    slack_bot = load_slack_bot_module()

    class CoordinatedFakeGenieClient(FakeGenieClient):
        def __init__(self):
            super().__init__()
            self.first_message_sent = threading.Event()
            self.release_first_response = threading.Event()

        def ask_question(self, text, conversation_id=None, on_message_sent=None):
            self.calls.append((text, conversation_id))
            self.counter += 1
            resolved_conversation_id = conversation_id or f"conv-{self.counter}"
            message_id = f"msg-{self.counter}"
            if on_message_sent is not None:
                on_message_sent(resolved_conversation_id, message_id)

            if text == "How many elections do we have?":
                self.first_message_sent.set()
                assert self.release_first_response.wait(timeout=2)

            return {
                "success": True,
                "conversation_id": resolved_conversation_id,
                "message_id": message_id,
                "response": f"response-{self.counter}",
                "result_data": None,
                "suggested_questions": [],
            }

    genie_client = CoordinatedFakeGenieClient()
    bot = slack_bot.SlackGenieBot(
        "xoxb-test",
        "secret",
        "xapp-test",
        genie_client,
    )
    client = FakeSlackClient()

    mention_event = {
        "channel": "C123",
        "channel_type": "channel",
        "ts": "400.100",
        "text": "<@U123BOT> How many elections do we have?",
    }
    reply_event = {
        "channel": "C123",
        "channel_type": "channel",
        "ts": "400.200",
        "thread_ts": "400.100",
        "text": "How many by year?",
    }

    root_thread = threading.Thread(
        target=bot._handle_message,
        args=(mention_event, None, client),
    )
    root_thread.start()

    assert genie_client.first_message_sent.wait(timeout=2)
    assert bot._should_handle_message_event(reply_event) is True

    bot._handle_message(reply_event, None, client)

    genie_client.release_first_response.set()
    root_thread.join(timeout=2)
    assert not root_thread.is_alive()

    assert genie_client.calls == [
        ("How many elections do we have?", None),
        ("How many by year?", "conv-1"),
    ]

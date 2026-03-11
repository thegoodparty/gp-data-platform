"""
Slack bot handler for Databricks Genie integration
"""

import logging
import re
from collections import OrderedDict
from threading import RLock
from time import monotonic
from typing import Any, Dict, List, Optional, Tuple

from databricks_genie_client import DatabricksGenieClient
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk import WebClient

logger = logging.getLogger(__name__)
SLACK_TEXT_LIMIT = 3900
MAX_CONVERSATION_THREADS = 1000
MAX_FEEDBACK_MESSAGES = 2000
MAX_RECENT_EVENT_KEYS = 2000


class SlackGenieBot:
    """Slack bot that interfaces with Databricks Genie"""

    def __init__(
        self,
        slack_bot_token: str,
        slack_signing_secret: str,
        slack_app_token: str,
        genie_client: DatabricksGenieClient,
    ):
        """Initialize the Slack Bolt app and in-memory conversation state."""
        self.app = App(
            token=slack_bot_token,
            signing_secret=slack_signing_secret,
        )
        self.slack_app_token = slack_app_token
        self.genie_client = genie_client
        self.client = WebClient(token=slack_bot_token)
        self.state_lock = RLock()

        # slack_thread_ts -> databricks_conversation_id
        self.conversation_map: OrderedDict[str, str] = OrderedDict()

        # message_ts -> (conversation_id, message_id) for feedback routing
        self.message_feedback_map: OrderedDict[str, Tuple[str, str]] = OrderedDict()

        # channel:message_ts -> monotonic timestamp for event de-duplication
        self.recent_event_keys: OrderedDict[str, float] = OrderedDict()

        self._register_handlers()

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    def _register_handlers(self):
        @self.app.event("app_mention")
        def handle_app_mention(event, say, client):
            self._handle_message(event, say, client)

        @self.app.event("message")
        def handle_message_events(event, say, client):
            if self._should_handle_message_event(event):
                self._handle_message(event, say, client)

        @self.app.action("feedback_positive")
        def handle_positive_feedback(ack, body, client):
            ack()
            self._handle_feedback(body, "positive", client)

        @self.app.action("feedback_negative")
        def handle_negative_feedback(ack, body, client):
            ack()
            self._handle_feedback(body, "negative", client)

    # ------------------------------------------------------------------
    # Core message handling
    # ------------------------------------------------------------------

    def _handle_message(self, event: Dict[str, Any], say, client):
        """Handle an inbound Slack message event."""
        thread_ts_value = event.get("thread_ts") or event.get("ts")
        try:
            text = event.get("text") or ""
            channel_value = event.get("channel")

            if event.get("bot_id"):
                return

            if not isinstance(channel_value, str) or not isinstance(
                thread_ts_value, str
            ):
                logger.warning(
                    "Skipping Slack event with missing channel or thread_ts: %s", event
                )
                return

            channel = channel_value
            thread_ts = thread_ts_value
            event_ts = event.get("ts")
            if isinstance(event_ts, str) and self._is_duplicate_event(
                channel, event_ts
            ):
                logger.info(
                    "Skipping duplicate Slack event for channel=%s ts=%s",
                    channel,
                    event_ts,
                )
                return

            text = self._clean_message_text(text)

            if not text.strip():
                self._post_message(
                    client,
                    channel=channel,
                    text="Please ask me a question about your data!",
                    thread_ts=thread_ts,
                )
                return

            # Post a thinking indicator that we will UPDATE in-place
            thinking_resp = self._post_message(
                client,
                channel=channel,
                text="Thinking...",
                thread_ts=thread_ts,
            )
            thinking_ts = thinking_resp.get("ts")

            # Get or create Databricks conversation
            conversation_id = self._get_mapping(self.conversation_map, thread_ts)

            logger.info(f"Asking Genie: {text}")
            result = self.genie_client.ask_question(text, conversation_id)

            # Store conversation mapping for thread continuity
            conversation_key = result.get("conversation_id")
            if isinstance(conversation_key, str):
                self._remember_mapping(
                    self.conversation_map,
                    thread_ts,
                    conversation_key,
                    MAX_CONVERSATION_THREADS,
                )

            # Format the response
            response_text = self._format_response(result)

            # ---- UPDATE the thinking message with the actual answer ----
            if thinking_ts:
                try:
                    self._update_message(
                        client,
                        channel=channel,
                        ts=thinking_ts,
                        text=response_text,
                    )
                except Exception as update_err:
                    logger.warning(
                        f"Failed to update thinking message, posting new: {update_err}"
                    )
                    self._post_message(
                        client,
                        channel=channel,
                        text=response_text,
                        thread_ts=thread_ts,
                    )
            else:
                self._post_message(
                    client,
                    channel=channel,
                    text=response_text,
                    thread_ts=thread_ts,
                )

            # Send query result data if available
            result_data = result.get("result_data")
            if result_data:
                data = result_data.get("data", {})
                if data.get("data_array"):
                    self._send_query_results(channel, thread_ts, result_data, client)

            # Send suggested follow-up questions
            suggested_questions = result.get("suggested_questions", [])
            if suggested_questions:
                self._send_suggested_questions(
                    channel, thread_ts, suggested_questions, client
                )

            # Send feedback buttons as the last message
            if result.get("success"):
                conv_id = result.get("conversation_id")
                msg_id = result.get("message_id")
                logger.info(
                    f"Preparing feedback buttons - conv_id: {conv_id}, msg_id: {msg_id}"
                )

                if isinstance(conv_id, str) and isinstance(msg_id, str):
                    feedback_msg = self._send_feedback_buttons(
                        channel, thread_ts, client
                    )
                    if feedback_msg:
                        fb_ts = feedback_msg.get("ts")
                        if isinstance(fb_ts, str):
                            self._remember_mapping(
                                self.message_feedback_map,
                                fb_ts,
                                (conv_id, msg_id),
                                MAX_FEEDBACK_MESSAGES,
                            )
                            logger.info(
                                f"Stored feedback mapping: {fb_ts} -> ({conv_id}, {msg_id})"
                            )
                else:
                    logger.warning(
                        "Missing conversation_id or message_id - cannot create feedback buttons"
                    )

        except Exception as e:
            logger.error(f"Error handling message: {e}", exc_info=True)
            if isinstance(channel_value, str) and isinstance(thread_ts_value, str):
                self._post_message(
                    client,
                    channel=channel_value,
                    text=(
                        "Sorry, I encountered an error processing your request. "
                        "Please try again."
                    ),
                    thread_ts=thread_ts_value,
                )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _clean_message_text(text: str) -> str:
        return re.sub(r"<@[A-Z0-9]+>", "", text).strip()

    @staticmethod
    def _truncate_for_slack(text: str) -> str:
        if len(text) <= SLACK_TEXT_LIMIT:
            return text
        return text[: SLACK_TEXT_LIMIT - len("\n\n_(truncated)_")] + "\n\n_(truncated)_"

    def _post_message(
        self,
        client,
        channel: str,
        text: str,
        thread_ts: Optional[str] = None,
        blocks: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "channel": channel,
            "text": self._truncate_for_slack(text),
        }
        if thread_ts:
            payload["thread_ts"] = thread_ts
        if blocks:
            payload["blocks"] = blocks
        return client.chat_postMessage(**payload)

    def _update_message(
        self,
        client,
        channel: str,
        ts: str,
        text: str,
        blocks: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "channel": channel,
            "ts": ts,
            "text": self._truncate_for_slack(text),
        }
        if blocks:
            payload["blocks"] = blocks
        return client.chat_update(**payload)

    def _should_handle_message_event(self, event: Dict[str, Any]) -> bool:
        if event.get("subtype") or event.get("bot_id"):
            return False
        if event.get("channel_type") == "im":
            return True
        thread_ts = event.get("thread_ts")
        return bool(
            isinstance(thread_ts, str)
            and self._has_mapping(self.conversation_map, thread_ts)
        )

    def _get_mapping(self, mapping: OrderedDict[str, Any], key: str) -> Optional[Any]:
        with self.state_lock:
            return mapping.get(key)

    def _has_mapping(self, mapping: OrderedDict[str, Any], key: str) -> bool:
        with self.state_lock:
            return key in mapping

    def _is_duplicate_event(self, channel: str, event_ts: str) -> bool:
        event_key = f"{channel}:{event_ts}"
        with self.state_lock:
            if event_key in self.recent_event_keys:
                self.recent_event_keys.move_to_end(event_key)
                return True
            self._remember_mapping(
                self.recent_event_keys,
                event_key,
                monotonic(),
                MAX_RECENT_EVENT_KEYS,
            )
            return False

    def _remember_mapping(
        self, mapping: OrderedDict[str, Any], key: str, value: Any, max_size: int
    ) -> None:
        with self.state_lock:
            mapping[key] = value
            mapping.move_to_end(key)
            while len(mapping) > max_size:
                mapping.popitem(last=False)

    @staticmethod
    def _format_response(result: Dict[str, Any]) -> str:
        if not result.get("success"):
            error = result.get("error", "Unknown error")
            return f"❌ {error}"
        response = result.get("response", "")
        return response if response else "✅ Query executed successfully"

    # ------------------------------------------------------------------
    # Rich message senders
    # ------------------------------------------------------------------

    def _send_query_results(self, channel, thread_ts, result_data, client):
        try:
            data = result_data.get("data", {})
            schema = result_data.get("schema", {})
            data_array = data.get("data_array", [])
            raw_row_count = data.get("row_count")
            if isinstance(raw_row_count, int):
                row_count = raw_row_count if raw_row_count > 0 else len(data_array)
            elif isinstance(raw_row_count, str) and raw_row_count.isdigit():
                row_count = int(raw_row_count)
            else:
                row_count = len(data_array)

            if not data_array:
                return

            columns = schema.get("columns", [])
            column_names = [
                col.get("name", f"col_{i}") for i, col in enumerate(columns)
            ]

            max_rows = 10
            table_text = self._format_data_array(column_names, data_array[:max_rows])

            msg = f"*Query Results:*\n```\n{table_text}\n```"
            if row_count > max_rows:
                msg += f"\n_Showing {max_rows} of {row_count} rows_"

            self._post_message(
                client,
                channel=channel,
                text=msg,
                thread_ts=thread_ts,
            )
        except Exception as e:
            logger.error(f"Error sending query results: {e}")

    def _send_suggested_questions(self, channel, thread_ts, questions, client):
        try:
            if not questions:
                return
            text = "*💡 Suggested follow-up questions:*\n"
            for i, q in enumerate(questions, 1):
                text += f"{i}. {q}\n"
            self._post_message(
                client,
                channel=channel,
                text=text,
                thread_ts=thread_ts,
            )
        except Exception as e:
            logger.error(f"Error sending suggested questions: {e}")

    def _send_feedback_buttons(
        self, channel: str, thread_ts: str, client
    ) -> Optional[dict]:
        try:
            blocks: List[Dict[str, Any]] = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*Was this response helpful?*",
                    },
                },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {
                                "type": "plain_text",
                                "text": "👍 Helpful",
                                "emoji": True,
                            },
                            "action_id": "feedback_positive",
                        },
                        {
                            "type": "button",
                            "text": {
                                "type": "plain_text",
                                "text": "👎 Not Helpful",
                                "emoji": True,
                            },
                            "action_id": "feedback_negative",
                        },
                    ],
                },
            ]
            return self._post_message(
                client,
                channel=channel,
                blocks=blocks,
                text="Was this response helpful?",
                thread_ts=thread_ts,
            )
        except Exception as e:
            logger.error(f"Error sending feedback buttons: {e}")
            return None

    # ------------------------------------------------------------------
    # Feedback handling
    # ------------------------------------------------------------------

    def _handle_feedback(self, body: Dict[str, Any], rating: str, client):
        """Route Slack feedback button clicks back to Genie."""
        try:
            message = body.get("message", {})
            msg_ts = message.get("ts")
            channel = body.get("channel", {}).get("id")
            user = body.get("user", {}).get("id")

            if not isinstance(msg_ts, str) or not isinstance(channel, str):
                logger.warning(
                    "Skipping feedback event with missing message ts or channel: %s",
                    body,
                )
                return

            logger.info(f"Feedback button clicked: {rating}, msg_ts: {msg_ts}")

            feedback_info = self._get_mapping(self.message_feedback_map, msg_ts)

            if not feedback_info:
                logger.warning(f"No feedback info found for message {msg_ts}")
                missing_feedback_blocks: List[Dict[str, Any]] = [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "⚠️ _Unable to submit feedback. Please try asking a new question._",
                        },
                    }
                ]
                self._update_message(
                    client,
                    channel=channel,
                    ts=msg_ts,
                    blocks=missing_feedback_blocks,
                    text="Unable to submit feedback",
                )
                return

            conversation_id, message_id = feedback_info
            logger.info(
                f"Sending feedback for conversation {conversation_id}, message {message_id}"
            )

            success = self.genie_client.send_message_feedback(
                conversation_id=conversation_id,
                message_id=message_id,
                rating=rating,
            )

            emoji = "👍" if rating == "positive" else "👎"
            if success:
                feedback_text = f"{emoji} _Thanks for your feedback!_"
                logger.info(
                    f"User {user} gave {rating} feedback for message {message_id}"
                )
            else:
                feedback_text = "❌ _Failed to submit feedback. Please try again._"
                logger.error("Failed to send feedback to Genie API")

            feedback_blocks: List[Dict[str, Any]] = [
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": feedback_text},
                }
            ]
            self._update_message(
                client,
                channel=channel,
                ts=msg_ts,
                blocks=feedback_blocks,
                text=feedback_text,
            )

        except Exception as e:
            logger.error(f"Error handling feedback: {e}", exc_info=True)

    # ------------------------------------------------------------------
    # Table formatting
    # ------------------------------------------------------------------

    def _format_data_array(
        self, column_names: List[str], data_array: List[list]
    ) -> str:
        if not data_array:
            return "No data"

        col_widths = []
        col_types = []

        for i in range(len(column_names)):
            max_width = len(column_names[i])
            is_numeric = True
            for row in data_array:
                cell = row[i] if i < len(row) else None
                val_str = str(cell) if cell is not None else ""
                max_width = max(max_width, len(val_str))
                if cell is not None and not self._is_numeric(cell):
                    is_numeric = False
            col_widths.append(min(max_width + 2, 30))
            col_types.append(is_numeric)

        lines = []

        header_parts = [
            name[:w].strip().center(w) for name, w in zip(column_names, col_widths)
        ]
        lines.append("│".join(header_parts))
        lines.append("┼".join("─" * w for w in col_widths))

        for row in data_array:
            parts = []
            for i, (w, num) in enumerate(zip(col_widths, col_types)):
                val = row[i] if i < len(row) else None
                s = str(val)[:w].strip() if val is not None else ""
                parts.append(
                    s.rjust(w) if num and self._is_numeric(val) else s.ljust(w)
                )
            lines.append("│".join(parts))

        return "\n".join(lines)

    @staticmethod
    def _is_numeric(value) -> bool:
        if value is None:
            return False
        try:
            float(str(value))
            return True
        except (ValueError, TypeError):
            return False

    # ------------------------------------------------------------------
    # Start
    # ------------------------------------------------------------------

    def start(self):
        """Start the Slack Socket Mode handler."""
        handler = SocketModeHandler(self.app, self.slack_app_token)
        logger.info("Starting Slack bot in socket mode...")
        handler.start()

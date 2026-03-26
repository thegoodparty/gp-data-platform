"""
Databricks Genie API Client for conversational interactions
"""

import logging
import re
import time
from typing import Any, Callable, Dict, Optional, Set, cast

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class DatabricksGenieClient:
    """Client for interacting with Databricks Genie conversational APIs."""

    def __init__(self, space_id: str):
        """Initialize the Genie client for a specific space."""
        self.space_id = space_id
        logger.info("Initializing Databricks SDK with OAuth M2M authentication")
        self.workspace_client = WorkspaceClient()
        self.api_client = self.workspace_client.api_client
        self.host = self.workspace_client.config.host.rstrip("/")

        logger.info("✓ Connected to Databricks workspace: %s", self.host)
        logger.info("✓ Using OAuth M2M authentication via app service principal")
        logger.info("✓ Genie space ID: %s", self.space_id)

    def get_console_url(self) -> str:
        """Return the URL for this Genie space in the Databricks console."""
        return f"{self.host}/genie/rooms/{self.space_id}"

    @staticmethod
    def _extract_status_code(error: Exception) -> Optional[int]:
        for attr in ("status_code", "http_status_code", "error_code"):
            value = getattr(error, attr, None)
            if isinstance(value, int):
                return value
            if isinstance(value, str) and value.isdigit():
                return int(value)

        response = getattr(error, "response", None)
        if response is not None:
            status_code = getattr(response, "status_code", None)
            if isinstance(status_code, int):
                return status_code

        match = re.search(r"\b([1-5]\d{2})\b", str(error))
        if match:
            return int(match.group(1))

        return None

    @staticmethod
    def _is_retryable_error(status_code: Optional[int], error: Exception) -> bool:
        if status_code in RETRYABLE_STATUS_CODES:
            return True

        if isinstance(error, (ConnectionError, TimeoutError)):
            return True

        message = str(error).lower()
        transient_tokens = (
            "timeout",
            "timed out",
            "connection reset",
            "connection aborted",
            "connection refused",
            "temporarily unavailable",
            "temporary failure in name resolution",
            "name or service not known",
            "dns",
        )
        return any(token in message for token in transient_tokens)

    def _make_request(
        self,
        method: str,
        path: str,
        data: Optional[Dict[str, Any]] = None,
        max_retries: int = 3,
        suppress_status_codes: Optional[Set[int]] = None,
        allow_empty_response: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """Make an authenticated API request using the SDK's HTTP client."""
        for attempt in range(max_retries):
            try:
                result = self.api_client.do(method, path, body=data)
                if result is None:
                    if allow_empty_response:
                        return {}
                    return None
                return cast(Dict[str, Any], result)
            except Exception as error:
                status_code = self._extract_status_code(error)
                is_retryable = self._is_retryable_error(status_code, error)

                if suppress_status_codes and status_code in suppress_status_codes:
                    logger.debug(
                        "Suppressed API error on %s %s (status=%s): %s",
                        method,
                        path,
                        status_code,
                        error,
                    )
                    return None

                if is_retryable and attempt < max_retries - 1:
                    wait_seconds = min(2**attempt, 8)
                    logger.warning(
                        "Retryable API error on %s %s (status=%s, attempt=%s/%s): %s. "
                        "Retrying in %ss",
                        method,
                        path,
                        status_code,
                        attempt + 1,
                        max_retries,
                        error,
                        wait_seconds,
                    )
                    time.sleep(wait_seconds)
                    continue

                logger.error(
                    "API request failed: %s %s (status=%s) - %s",
                    method,
                    path,
                    status_code,
                    error,
                    exc_info=True,
                )
                return None
        return None

    def send_message(
        self, conversation_id: Optional[str], message: str
    ) -> Optional[Dict[str, Any]]:
        """
        Send a message to the Genie space

        Args:
            conversation_id: The conversation ID (optional, for continuing a conversation)
            message: The user's message/question

        Returns:
            Message response dict if successful, None otherwise
        """
        # Determine the correct endpoint based on whether this is a new or existing conversation
        if conversation_id:
            # Continue existing conversation
            path = f"/api/2.0/genie/spaces/{self.space_id}/conversations/{conversation_id}/messages"
        else:
            # Start new conversation
            path = f"/api/2.0/genie/spaces/{self.space_id}/start-conversation"

        payload = {"content": message}

        result = self._make_request("POST", path, data=payload)

        if not result:
            return None

        message_id = result.get("message_id") or result.get("id")

        # Extract message details if nested
        message_data = result.get("message", {})
        if message_data:
            actual_conv_id = message_data.get("conversation_id")
            message_id = message_data.get("id") or message_id
        else:
            actual_conv_id = result.get("conversation_id", conversation_id)

        logger.info("Sent message %s to conversation %s", message_id, actual_conv_id)

        # Return the message data with conversation_id at top level for easy access
        return {
            "id": message_id,
            "message_id": message_id,
            "conversation_id": actual_conv_id,
            "status": message_data.get("status") or result.get("status"),
            "content": message_data.get("content") or result.get("content"),
            "raw_response": result,
        }

    def get_message_status(
        self, conversation_id: str, message_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get the status and response of a message

        Args:
            conversation_id: The conversation ID
            message_id: The message ID

        Returns:
            Message status dict if successful, None otherwise
        """
        path = f"/api/2.0/genie/spaces/{self.space_id}/conversations/{conversation_id}/messages/{message_id}"

        result = self._make_request("GET", path)

        if not result:
            return None

        # Handle both direct message response and nested message data
        if "message" in result:
            return result["message"]
        return result

    def wait_for_response(
        self,
        conversation_id: str,
        message_id: str,
        max_wait_time: int = 120,
        poll_interval: int = 2,
        max_poll_interval: int = 5,
        max_empty_polls: int = 3,
    ) -> Optional[Dict[str, Any]]:
        """
        Poll for a message response until it's complete or timeout

        Args:
            conversation_id: The conversation ID
            message_id: The message ID
            max_wait_time: Maximum time to wait in seconds
            poll_interval: Time between polls in seconds
            max_empty_polls: Consecutive empty poll results allowed before aborting

        Returns:
            Complete message response dict if successful, None otherwise
        """
        start_time = time.time()
        current_poll_interval = poll_interval
        empty_polls = 0

        while time.time() - start_time < max_wait_time:
            status = self.get_message_status(conversation_id, message_id)

            if not status:
                empty_polls += 1
                logger.warning(
                    "Empty poll result for message %s (attempt %s/%s)",
                    message_id,
                    empty_polls,
                    max_empty_polls,
                )
                if empty_polls >= max_empty_polls:
                    return None
                time.sleep(current_poll_interval)
                current_poll_interval = min(
                    current_poll_interval + 1, max_poll_interval
                )
                continue

            empty_polls = 0

            # Check if the response is complete
            state = status.get("status")

            if state == "COMPLETED":
                logger.info("Message %s completed", message_id)
                return status
            elif state in ["FAILED", "CANCELLED"]:
                logger.error("Message %s failed with state: %s", message_id, state)
                return status

            time.sleep(current_poll_interval)
            current_poll_interval = min(current_poll_interval + 1, max_poll_interval)

        logger.warning("Timeout waiting for message %s", message_id)
        return None

    def get_message_attachment_query_result(
        self, conversation_id: str, message_id: str, attachment_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get query result data for a Genie message attachment."""
        paths = [
            (
                f"/api/2.0/genie/spaces/{self.space_id}/conversations/"
                f"{conversation_id}/messages/{message_id}/attachments/{attachment_id}/query-result"
            ),
            (
                f"/api/2.0/genie/spaces/{self.space_id}/conversations/"
                f"{conversation_id}/messages/{message_id}/query-result/{attachment_id}"
            ),
        ]

        for path in paths:
            result = self._make_request("GET", path, suppress_status_codes={404})
            if result:
                logger.info(
                    "Retrieved Genie query result for conversation=%s message=%s attachment=%s",
                    conversation_id,
                    message_id,
                    attachment_id,
                )
                return result

        logger.warning(
            "No query result payload returned for conversation=%s message=%s attachment=%s",
            conversation_id,
            message_id,
            attachment_id,
        )
        return None

    def ask_question(
        self,
        question: str,
        conversation_id: Optional[str] = None,
        on_message_sent: Optional[Callable[[str, str], None]] = None,
    ) -> Dict[str, Any]:
        """
        High-level method to ask a question and get the response

        Args:
            question: The question to ask
            conversation_id: Optional existing conversation ID. If None, creates a new one.
            on_message_sent: Optional callback invoked with (conversation_id,
                message_id) after the message is sent successfully but before
                polling for the full response. Used to persist conversation
                mappings early and avoid fast follow-up races.

        Returns:
            Dict with 'success', 'conversation_id', 'response', and 'attachments' keys
        """
        # Send the message (conversation will be created implicitly if conversation_id is None)
        message_result = self.send_message(conversation_id, question)
        if not message_result:
            return {
                "success": False,
                "conversation_id": conversation_id,
                "error": "Failed to send message",
            }

        # Get the actual conversation ID from the response (API might return it)
        actual_conversation_id = message_result.get("conversation_id", conversation_id)
        message_id = message_result.get("id") or message_result.get("message_id")

        if not isinstance(actual_conversation_id, str) or not isinstance(
            message_id, str
        ):
            return {
                "success": False,
                "conversation_id": conversation_id,
                "error": "Genie response missing conversation_id or message_id",
            }

        if on_message_sent is not None:
            try:
                on_message_sent(actual_conversation_id, message_id)
            except Exception as callback_error:
                logger.warning(
                    "on_message_sent callback failed for conversation=%s message=%s: %s",
                    actual_conversation_id,
                    message_id,
                    callback_error,
                )

        # Wait for the response
        response = self.wait_for_response(actual_conversation_id, message_id)

        if not response:
            return {
                "success": False,
                "conversation_id": actual_conversation_id,
                "error": "Failed to get response or timeout",
            }

        # Extract the response content
        status = response.get("status")

        if status == "COMPLETED":
            # Extract text response and any attachments
            raw_attachments = response.get("attachments")
            attachments = raw_attachments if isinstance(raw_attachments, list) else []
            query_result = response.get("query_result")

            response_text = ""
            query_attachment_id = None
            sql_text = None

            if attachments:
                for attachment in attachments:
                    if not isinstance(attachment, dict):
                        continue

                    text_attachment = attachment.get("text")
                    if isinstance(text_attachment, dict):
                        text_content = text_attachment.get("content") or ""
                        if text_content:
                            response_text += text_content + "\n\n"

                    query_attachment = attachment.get("query")
                    if isinstance(query_attachment, dict):
                        query_data = query_attachment
                        description = query_data.get("description") or ""
                        attachment_id = attachment.get("attachment_id")
                        if isinstance(attachment_id, str):
                            query_attachment_id = attachment_id

                        # Extract the generated SQL query
                        raw_sql = query_data.get("query")
                        if isinstance(raw_sql, str) and raw_sql.strip():
                            sql_text = raw_sql.strip()

                        if description:
                            response_text += description + "\n\n"

            result_data = None
            if query_attachment_id:
                attachment_result = self.get_message_attachment_query_result(
                    actual_conversation_id,
                    message_id,
                    query_attachment_id,
                )
                if attachment_result:
                    statement_response = attachment_result.get(
                        "statement_response", attachment_result
                    )
                    result_data = {
                        "data": statement_response.get("result", {}),
                        "schema": statement_response.get("manifest", {}).get(
                            "schema", {}
                        ),
                    }

            if not response_text.strip():
                content = response.get("content")
                response_text = (
                    content
                    if isinstance(content, str) and content
                    else "No response generated"
                )

            raw_suggested_questions = response.get("suggested_questions")
            suggested_questions = (
                raw_suggested_questions
                if isinstance(raw_suggested_questions, list)
                else []
            )

            return {
                "success": True,
                "conversation_id": actual_conversation_id,
                "message_id": message_id,
                "response": response_text.strip(),
                "attachments": attachments,
                "query_result": query_result,
                "result_data": result_data,
                "suggested_questions": suggested_questions,
                "sql_text": sql_text,
            }
        else:
            raw_error = response.get("error")
            if isinstance(raw_error, dict):
                error_message = raw_error.get("message")
                error_msg = (
                    error_message if isinstance(error_message, str) else "Unknown error"
                )
            elif isinstance(raw_error, str) and raw_error:
                error_msg = raw_error
            else:
                error_msg = "Unknown error"
            return {
                "success": False,
                "conversation_id": actual_conversation_id,
                "error": f"Query failed: {error_msg}",
            }

    def send_message_feedback(
        self,
        conversation_id: str,
        message_id: str,
        rating: str,
        feedback_text: Optional[str] = None,
    ) -> bool:
        """
        Send feedback for a message

        Args:
            conversation_id: The conversation ID
            message_id: The message ID
            rating: Feedback rating - "positive", "negative", or "none" (case-insensitive)
            feedback_text: Optional feedback text

        Returns:
            True if feedback was sent successfully, False otherwise
        """
        path = f"/api/2.0/genie/spaces/{self.space_id}/conversations/{conversation_id}/messages/{message_id}/feedback"

        # API requires uppercase rating values: POSITIVE, NEGATIVE, or NONE
        payload = {"rating": rating.upper()}

        if feedback_text:
            payload["feedback_text"] = feedback_text

        result = self._make_request(
            "POST",
            path,
            data=payload,
            allow_empty_response=True,
        )

        if result is not None:
            logger.info("Sent %s feedback for message %s", rating.upper(), message_id)
            return True

        return False

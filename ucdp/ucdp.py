import collections
import json
import logging
import queue
import threading

from .data import UcdpData
from .event import UcdpEvent

from typing import Callable, Iterable
EventCallback = Callable[[UcdpEvent], None]

class NoSenderSetException(Exception):
	def __init__(self):
		super().__init__("No sender set, use set_sender")

class Ucdp:
	LOGGER_NAME = 'Ucdp'
	METHOD_LOGGER_NAME = 'Ucdp.method'
	EVENT_LOGGER_NAME = 'Ucdp.event'

	def __init__(self, use_event_thread=True):
		self.use_event_thread = use_event_thread

		self.data = UcdpData()

		self._logger = logging.getLogger(self.LOGGER_NAME)
		self._method_logger = logging.getLogger(self.METHOD_LOGGER_NAME)
		self._event_logger = logging.getLogger(self.EVENT_LOGGER_NAME)

		self._sender = None
		self._all_events_subscribers = []
		self._event_subscribers = collections.defaultdict(list)
		self._pending_results = {}
		self._next_msg_id = 1

		if self.use_event_thread:
			self._event_queue = queue.SimpleQueue()
			threading.Thread(name='Ucdp::_event_handler_caller', target=self._event_handler_caller, daemon=True).start()

	def set_sender(self, sender: Callable[[str], None]):
		self._sender = sender

	def process_message(self, message: str):
		msg = json.loads(message)
		if 'method' in msg:
			event = UcdpEvent(name=msg['method'], params=msg['params'])
			self._process_event(event)
		elif 'result' in msg:
			self._process_result(msg['id'], msg['result'])
		else:
			self._logger.warning("Unknown message format, ignoring: %s", msg)

	def subscribe_events_decorator(self, events: None | str | Iterable[str] = None) -> Callable[[EventCallback], EventCallback]:
		def wrapper(f):
			self.subscribe_events(f, events)
			return f
		return wrapper

	def subscribe_events(self, cb: EventCallback, events: None | str | Iterable[str] = None):
		if events is None:
			self._all_events_subscribers.append(cb)
		elif isinstance(events, str):
			self._event_subscribers[events].append(cb)
		elif hasattr(events, '__iter__'):
			for event in events:
				self._event_subscribers[event].append(cb)
		else:
			self._event_subscribers[events].append(cb)

	def call_nowait(self, method: str, **params):
		msg = self._get_msg(method, params)
		self._send_msg(msg)

	def call(self, method: str, **params):
		msg = self._get_msg(method, params)
		q = queue.SimpleQueue()
		self._pending_results[msg['id']] = q
		self._send_msg(msg)
		result = q.get()
		del self._pending_results[msg['id']]
		return result

	def _get_msg(self, method: str, params: dict):
		msg = {
			'id': self._next_msg_id,
			'method': method,
			'params': params,
		}
		self._next_msg_id += 1
		return msg

	def _send_msg(self, msg: dict):
		if self._sender is None:
			raise NoSenderSetException()

		self._method_logger.debug("<- Method %s: %s %s", msg['id'], msg['method'], msg['params'])
		self._sender(json.dumps(msg))

	def _process_result(self, result_id: int, result: dict):
		self._method_logger.debug("-> Result %s: %s", result_id, result)

		pending = self._pending_results.get(result_id, None)
		if pending is None:
			self._logger.warning("Received result %s with no waiters: %s", result_id, result)
		else:
			pending.put(result)

	def _process_event(self, event: UcdpEvent):
		self._event_logger.debug("-> Event %s: %s", event.name, event.params)
		self.data._process_event(event)
		if self.use_event_thread:
			self._event_queue.put(event)
		else:
			self._emit_event(event)

	def _event_handler_caller(self):
		while True:
			self._emit_event(self._event_queue.get())

	def _emit_event(self, event):
		for sub in self._all_events_subscribers:
			sub(event)

		for sub in self._event_subscribers.get(event.name, []):
			sub(event)

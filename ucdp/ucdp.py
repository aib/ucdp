import collections
import json
import logging
import queue

from .data import UcdpData
from .event import UcdpEvent

from typing import Callable, Iterable
EventCallback = Callable[[UcdpEvent], None]

class NoSenderSetException(Exception):
	def __init__(self):
		super().__init__("No sender set, use set_sender")

class Ucdp:
	def __init__(self):
		self.logger = logging.getLogger('Ucdp')
		self.method_logger = logging.getLogger('Ucdp.method')
		self.event_logger = logging.getLogger('Ucdp.event')
		self.sender = None
		self.data = UcdpData()
		self.event_subscribers = collections.defaultdict(list)
		self.all_events_subscribers = []
		self.pending_results = {}
		self.next_msg_id = 1

	def set_sender(self, sender: Callable[[str], None]):
		self.sender = sender

	def process_message(self, message: str):
		msg = json.loads(message)
		if 'method' in msg:
			event = UcdpEvent(name=msg['method'], params=msg['params'])
			self._process_event(event)
		elif 'result' in msg:
			self._process_result(msg['id'], msg['result'])
		else:
			self.logger.warning("Unknown message format, ignoring: %s", msg)

	def subscribe_events_decorator(self, events: None | str | Iterable[str] = None) -> Callable[[EventCallback], EventCallback]:
		def wrapper(f):
			self.subscribe_events(f, events)
			return f
		return wrapper

	def subscribe_events(self, cb: EventCallback, events: None | str | Iterable[str] = None):
		if events is None:
			self.all_events_subscribers.append(cb)
		elif isinstance(events, str):
			self.event_subscribers[events].append(cb)
		elif hasattr(events, '__iter__'):
			for event in events:
				self.event_subscribers[event].append(cb)
		else:
			self.event_subscribers[events].append(cb)

	def call(self, method: str, **params):
		msg = self._get_msg(method, params)
		q = queue.SimpleQueue()
		self.pending_results[msg['id']] = q
		self._send_msg(msg)
		result = q.get()
		del self.pending_results[msg['id']]
		return result

	def _get_msg(self, method: str, params: dict):
		msg = {
			'id': self.next_msg_id,
			'method': method,
			'params': params,
		}
		self.next_msg_id += 1
		return msg

	def _send_msg(self, msg: dict):
		if self.sender is None:
			raise NoSenderSetException()

		self.method_logger.debug("<- Method %s: %s %s", msg['id'], msg['method'], msg['params'])
		self.sender(json.dumps(msg))

	def _process_result(self, result_id: int, result: dict):
		self.method_logger.debug("-> Result %s: %s", result_id, result)

		pending = self.pending_results.get(result_id, None)
		if pending is None:
			self.logger.warning("Received result %s with no waiters: %s", result_id, result)
		else:
			pending.put(result)

	def _process_event(self, event: UcdpEvent):
		self.event_logger.debug("-> Event %s: %s", event.name, event.params)
		self.data._process_event(event)
		self._emit_event(event)

	def _emit_event(self, event: UcdpEvent):
		for sub in self.all_events_subscribers:
			sub(event)

		for sub in self.event_subscribers.get(event.name, []):
			sub(event)

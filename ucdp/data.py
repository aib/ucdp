from .event import UcdpEvent

class UcdpData:
	def __init__(self):
		self.scripts = {}

	def _process_event(self, event: UcdpEvent):
		if event.name == 'Debugger.scriptParsed':
			self.scripts[event.params['scriptId']] = event.params

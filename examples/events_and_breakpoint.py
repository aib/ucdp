import threading
import time

import requests
import ucdp
import websockets.sync.client

def recv_thread(cdp, ws):
	try:
		while True:
			cdp.process_message(ws.recv())
	except websockets.exceptions.ConnectionClosedOK:
		pass

def on_cdp_event(ev):
	if ev.name == 'Debugger.scriptParsed':
		if not ev.params['url'].startswith('node:'):
			print(f"Script: {ev.params['url']}")
		return
	else:
		print(f"Event: {ev.name}")

def main():
	cdp = ucdp.Ucdp()
	target = requests.get(f'http://127.0.0.1:9229/json').json()[0]

	with websockets.sync.client.connect(target['webSocketDebuggerUrl']) as ws:
		threading.Thread(name='ws receiver', target=recv_thread, args=(cdp, ws), daemon=True).start()

		cdp.set_sender(ws.send)

		cdp.subscribe_events(on_cdp_event)

		@cdp.subscribe_events_decorator('Debugger.paused')
		def debugger_paused(ev):
			print(f"Debugger paused because of << {ev.params['reason']} >>")

			if ev.params['reason'] == 'Break on start':
				print("Scripts:")
				for script in filter(lambda s: not s['url'].startswith('node:'), cdp.data.scripts.values()):
					print(f"  - {script['scriptId']}: {script['url']}")

				cdp.call('Debugger.setPauseOnExceptions', state='all')
				bp = cdp.call('Debugger.setBreakpointByUrl', urlRegex=r'main\.js', lineNumber=10)
				print(f"Set breakpoint -> {bp}")

				cdp.call('Debugger.resume')
				return

			def callframe_to_str(cf):
				def location_to_str(loc):
					return f"{cdp.data.scripts[loc['scriptId']]['url']}:{loc['lineNumber']+1}:{loc['columnNumber']+1}"
				return f"{location_to_str(cf['location'])} {cf['functionName']}"

			print("Call frames:")
			for callframe in ev.params['callFrames']:
				print(f"  - {callframe_to_str(callframe)}")

			print("Resuming in 3...")
			time.sleep(3)
			cdp.call('Debugger.resume')

		cdp.call('Runtime.enable')
		cdp.call('Debugger.enable')
		cdp.call('Runtime.runIfWaitingForDebugger')

		time.sleep(30)
		print("Quitting...")

if __name__ == '__main__':
	main()

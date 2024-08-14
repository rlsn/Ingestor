"""
This script runs the webui server
rlsn 2024
"""
from nicegui import ui
import pygestor.webui.dataviewer as dataviewer
from pygestor.webui.new_menu import add_new_dataset
ui.page_title('Pygestor')

with ui.header(elevated=True).style('background-color: bg-blue-100').classes('items-center justify-between px-3 py-1 gap-0') as header:
    with ui.row():
        ui.button('Home', icon='home', on_click=lambda: dataviewer.show(body)).classes('items-center px-3')
        ui.button('New', icon="add", on_click=add_new_dataset)
    with ui.link(target='https://github.com/rlsn/Pygestor').classes('p-0'):
        ui.add_head_html('<link href="https://unpkg.com/eva-icons@1.1.3/style/eva-icons.css" rel="stylesheet" />')
        ui.icon('eva-github', color='white').classes('rounded-full text-4xl')
        
body=ui.row().classes('w-full no-wrap')

dataviewer.show(body)
ui.run(reload=False)
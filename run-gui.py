"""
This script runs the gui server
rlsn 2024
"""
from nicegui import ui
import pygestor.gui.dataviewer as dataviewer
ui.page_title('Pygestor')

with ui.header(elevated=True).style('background-color: bg-blue-100').classes('items-center justify-between px-3 py-1 gap-0'):
        ui.button('Home', icon='home', on_click=lambda: dataviewer.show(body)).classes('items-center px-3')
        
        with ui.link(target='https://github.com/rlsn').classes('p-0'):
            ui.add_head_html('<link href="https://unpkg.com/eva-icons@1.1.3/style/eva-icons.css" rel="stylesheet" />')
            ui.icon('eva-github', color='white').classes('rounded-full text-4xl')
        
body=ui.row().classes('w-full no-wrap')

dataviewer.show(body)
ui.run(reload=False)
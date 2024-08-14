"""
This script shows ui for adding new datasets
rlsn 2024
"""
from nicegui import ui
import asyncio
from pygestor.dataset_wrapper import Dataset
from pygestor.core_api import initialize_dataset

_view_classes = 'items-center w-[500px] h-[500px]'
_view_style = 'max-width: none; max-height: none'
def add_new_dataset(context):
    async def on_add(name, data_cls, url):
        n = ui.notification(timeout=None)
        n.message = 'Processing metadata ...'
        n.spinner = True
        await asyncio.sleep(0.1)
        async def coro():
            return await asyncio.to_thread(initialize_dataset, name, data_cls, url=url, verbose=True)

        view.close()

        task = asyncio.create_task(coro())
        ret = await task
        from pygestor.webui.dataviewer import show_datasets
        show_datasets()
        
        n.message = 'Success!' if ret else 'Failed to add the dataset, see terminal for more info'
        n.spinner = False
        await asyncio.sleep(2)
        n.dismiss()
    

    with ui.dialog() as view, ui.card().classes(_view_classes).style(_view_style):
        with ui.column().classes('px-5 w-full'):
            with ui.row():
                ui.label("Adding A New Dataset").style('font-size:150%; font-weight:bold')
            with ui.column().classes('w-full gap-5'):
                name = ui.input(label="Dataset Name", placeholder="A unique name for your dataset").classes("w-full")
                url = ui.input(label="Dataset URL", placeholder='e.g. https://huggingface.co/datasets/<repo_id>').classes("w-full")

                cls_options = Dataset.get_abstract_names()
                data_cls = ui.select(label="Pipline", value=cls_options[0], options=cls_options).classes("w-full")

                ui.button("Add dataset", icon="add", on_click=lambda: on_add(name.value,data_cls.value, url.value)).classes("w-full")

        
    view.open()
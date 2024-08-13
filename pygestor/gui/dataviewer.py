"""
This script contains functions for main display
rlsn 2024
"""
import pyperclip
from nicegui import ui
import asyncio
from pygestor import load_meta, DATA_DIR, download, remove, stream_dataset, process_samples
from pygestor.utils import read_schema, AttrDict
from pygestor.gui.infoview import *
from pygestor.gui.gui_utils import stream_load_code_snipet, full_load_code_snipet
from pygestor.gui import gui_config

_title_style = 'color: black; font-size: 180%; font-weight: bold; '
_path = [None, None]
_search_value = [""]
_multi_sel = []

def reinit():
    _path[0], _path[1] = None, None
    _search_value[0] = ""
    _multi_sel.clear()

def dataset_table(metadata):
    columns = [
        {'name': 'name', 'label': 'Dataset', 'field': 'name', 'required': True, 'align': 'left', 'sortable': True},
        {'name': 'modality', 'label': 'Modality', 'field': 'modality', 'sortable': True},
        {'name': 'src', 'label': 'Source', 'field': 'src', 'align': 'left'},
        {'name': 'desc', 'label': 'Description', 'field': 'desc', 'align': 'left', 'style': 'text-wrap: wrap;'},
    ]
    rows = []
    for ds in metadata["datasets"]:
        if _search_value[0]!="" and _search_value[0] not in ds:
            continue
        rows.append({
            'name': ds, 
            'modality': ",".join(metadata["datasets"][ds]["modality"]),
            'desc': metadata["datasets"][ds]["description"],
            'src': metadata["datasets"][ds]["source"],
        })

    return columns, rows

def subset_table(metadata):
    columns = [
        {'name': 'name', 'label': 'Subset', 'field': 'name', 'required': True, 'align': 'left', 'sortable': True},
        {'name': 'size', 'label': 'Size (MB)', 'field': 'size', 'sortable': True},
        {'name': 'downloaded', 'label': 'Downloaded', 'field': 'downloaded', 'sortable': True},
        {'name': 'partitions', 'label': 'Partitions', 'field': 'partitions', 'sortable': True},
        {'name': 'desc', 'label': 'Description', 'field': 'desc', 'align': 'left', 'style': 'text-wrap: wrap;'},
    ]
    rows = []
    for subs in metadata["subsets"]:
        if _search_value[0]!="" and _search_value[0] not in subs:
            continue
        info = metadata["subsets"][subs]
        downloaded = compute_subset_download(info)
        parts = info['partitions']
        rows.append({
            'name': subs, 
            'size': round(compute_subset_size(info)/1e6,3),
            'downloaded': downloaded,
            'partitions': len(parts),
            'desc': info["description"],
        })

    return columns, rows


def partition_table(metadata):
    columns = [
        {'name': 'name', 'label': 'Partition', 'field': 'name', 'required': True, 'align': 'left', 'sortable': True},
        {'name': 'size', 'label': 'Size (MB)', 'field': 'size', 'sortable': True},
        {'name': 'downloaded', 'label': 'Downloaded', 'field': 'downloaded', 'sortable': True},
    ]
    rows = []
    for part in metadata["partitions"]:
        if _search_value[0]!="" and _search_value[0] not in part:
            continue
        info = metadata["partitions"][part]
        downloaded = info["downloaded"]
        rows.append({
            'name': part, 
            'size': round(info["size"]/1e6,3),
            'downloaded': 'Yes' if downloaded else 'No',
        })

    return columns, rows

def show_partition_info(views, metadata, path):
    name, subs, part = path
    infoview = views["infoview"]
    infoview.clear()

    async def on_download():
        n = ui.notification(timeout=None)
        n.message = 'Downloading : See progress on the server'
        n.spinner = True
        await asyncio.sleep(0.1)
        async def coro():
            if len(_multi_sel)>0:
                download(name, subset=subs, partitions=_multi_sel, force_redownload=False)
            else:
                download(name, subset=subs, partitions=[part], force_redownload=False)
        task = asyncio.create_task(coro())
        await task

        n.message = 'Done!'
        n.spinner = False
        metadata["datasets"]=load_meta()["datasets"]
        show_partitions(views, metadata)
        await asyncio.sleep(2)
        n.dismiss()

    async def on_remove():
        n = ui.notification(timeout=None)
        n.message = 'Removing data ...'
        n.spinner = True
        await asyncio.sleep(0.1)
        async def coro():
            if len(_multi_sel)>0:
                remove(name, subset=subs, partitions=_multi_sel, force_remove=True)
            else:
                remove(name, subset=subs, partitions=[part], force_remove=True)
        task = asyncio.create_task(coro())
        await task

        n.message = 'Done!'
        n.spinner = False
        metadata["datasets"]=load_meta()["datasets"]
        show_partitions(views, metadata)
        await asyncio.sleep(2)
        n.dismiss()

    info = metadata["datasets"][name]["subsets"][subs]["partitions"][part]

    miniviews = dict()
    def show_buttons():
        miniviews["buttonsview"].clear()
        with miniviews["buttonsview"]:
            if not info["downloaded"]:
                ui.button("Download all" if len(_multi_sel)>0 else "Download" ,icon="download", on_click=on_download).classes("w-full")
            else:
                ui.button("Remove all" if len(_multi_sel)>0 else "Remove data",icon="delete", on_click=dialog.open).classes("w-full").props(f'color=red')
            ui.button("Loader Snipet",icon="code", on_click=snipet.open).classes("w-full")
            
    def show_info():
        miniviews["listview"].clear()
        with miniviews["listview"]:
            if len(_multi_sel)>0:
                ui.label(f"{len(_multi_sel)} partitions selected").classes('p-3')
            else:
                show_partition_info_list(metadata, (name, subs, part))
    def on_multsel_change(partitions):
        _multi_sel.clear()
        for x in partitions:
            _multi_sel.append(x)

        if len(_multi_sel)>0:
            codemirrors[0].set_value(stream_load_code_snipet(name,subs,_multi_sel))
            codemirrors[1].set_value(full_load_code_snipet(name,subs,_multi_sel))
        else:
            codemirrors[0].set_value(stream_load_code_snipet(name,subs,[part]))
            codemirrors[1].set_value(full_load_code_snipet(name,subs,[part]))
        
        show_buttons()
        show_info()

    with infoview:
        with ui.dialog() as dialog, ui.card().classes('items-center'):
            ui.label('You are about to remove the downloaded data.')
            ui.markdown('**Are you sure?**')
            with ui.grid(columns=2).classes('w-full items-center'):
                ui.button('Yes', icon="delete", on_click=on_remove).classes('w-full').props(f'color=red')
                ui.button('No', on_click=dialog.close).classes('w-full')

        tab_names = ['Stream', 'Full']
        with ui.dialog() as snipet, ui.card().classes('items-center').style('width: 600px; height: 400px; max-width: none; max-height: none'):
            codemirrors = []
            with ui.tabs().classes('w-full') as tabs:
                codemirrors.append(ui.tab(tab_names[0]).classes('w-full'))
                codemirrors.append(ui.tab(tab_names[1]).classes('w-full'))
            with ui.tab_panels(tabs, value=tab_names[0]).classes('w-full'):
                with ui.tab_panel(tab_names[0]).classes('w-full'):
                    codemirrors[0] = ui.codemirror(
                        stream_load_code_snipet(name,subs,[part]),
                                              line_wrapping=True, language='Python').classes('h-full')
                with ui.tab_panel(tab_names[1]).classes('w-full'):
                    codemirrors[1] = ui.codemirror(
                        full_load_code_snipet(name,subs,[part]), 
                                              line_wrapping=True, language='Python').classes('h-full')

            def on_click():
                pyperclip.copy(codemirrors[tab_names.index(tabs.value)].value)
                ui.notify("Copied to clipboard")
                snipet.close()
            ui.button('Copy', 
                    on_click=on_click
                    ).classes('w-2/3').style('position: absolute; bottom: 10px;')

        with ui.column().classes("w-full items-center p-5"):
            buttonsview = ui.column().classes("w-full items-center gap-2")
            miniviews["buttonsview"] = buttonsview

            _,rows=partition_table(metadata["datasets"][_path[0]]["subsets"][_path[1]])
            names = [r["name"] for r in rows]
            ui.select(names, multiple=True, clearable=True, value=_multi_sel if len(_multi_sel)>0 else [part], label='Multiple selection', on_change=lambda x:on_multsel_change(x.value)).classes('w-full')
        listview = ui.column().classes("w-full items-center")
        miniviews["listview"] = listview
        show_buttons()
        show_info()


def show_partitions(views, metadata):
    name, subs = _path
    dataview, infoview = views["dataview"], views["infoview"]
    dataview.clear()
    infoview.clear()

    def on_click_back(views, metadata):
        _path[1]=None
        _search_value[0] = ""
        _multi_sel.clear()
        show_sesarch_bar(views, metadata)
        show_subsets(views, metadata)

    def on_click_row(views, metadata, path):
        _multi_sel.clear()
        show_partition_info(views, metadata, path)

    columns, rows = partition_table(metadata["datasets"][name]["subsets"][subs])
    with dataview:
        with ui.row():
            backbutton = ui.button(icon="arrow_back", on_click=lambda: on_click_back(views, metadata))
            ui.label(f"> {name} > {subs}").style(_title_style)
        table = ui.table(columns=columns, rows=rows, 
                         row_key='name',
                         pagination={'rowsPerPage': 10, 'sortBy': 'downloaded', 'descending':True, 'page': 1}
                         ).classes('w-full')
        table.on('rowClick', lambda e: on_click_row(views, metadata, (name, subs, e.args[-2]['name'])))


def show_schema(views, metadata, path):
    import glob
    from PIL.JpegImagePlugin import JpegImageFile

    name, subs = path
    infoview = views["infoview"]
    infoview.clear()
    info = metadata["datasets"][name]["subsets"][subs]

    download_path = os.path.abspath(os.path.join(DATA_DIR, info["path"]))
    parquet_file = glob.glob(download_path+"/*")[0]
    schema = read_schema(parquet_file)
    datastream = iter(stream_dataset(name, subs, download_if_missing=False, 
                                     batch_size = gui_config.n_preview_samples))
    batch = [process_samples(name, next(datastream))]

    def on_click_sample(index):
        iloc = AttrDict(value=1)
        sample_content.clear()

        field_ids = [index] if index>-1 else list(range(len(schema.names)))
        def fill_content(fields=None):
            gridview.clear()
            if fields is None:
                fields = [schema.names[i] for i in field_ids]
            else:
                field_ids.clear()
                field_ids.extend([schema.names.index(f) for f in fields])
            with gridview:
                ui.label("Field").style('font-weight: bold')
                ui.label("Sample").style('font-weight: bold')
                for field in fields:
                    ui.label(field)
                    sample = batch[0][field][iloc.value-1]
                    if type(sample)==JpegImageFile:
                        ui.image(sample).classes('w-[300px]')
                    else:
                        ui.label(str(sample))

        def next_batch():
            batch[0] = process_samples(name, next(datastream))
            fill_content()

        with sample_content:
            with ui.scroll_area().classes('w-full h-[500px]'):
                gridview = ui.grid(columns="auto 1fr").classes('w-full no-wrap')
            with ui.row().classes('w-full items-top no-wrap h-auto p-1'):
                ui.select(schema.names, multiple=True, 
                          value=[schema.names[i] for i in field_ids], label='fields',
                          on_change=lambda e:fill_content(e.value)).classes('w-1/3').props('use-chips')
                with ui.row().classes('w-2/3 items-center no-wrap h-auto p-3'):
                    ui.slider(min=1, max=gui_config.n_preview_samples, on_change=fill_content).bind_value(iloc, 'value').classes('w-2/3 px-2 py-0')
                    ui.number(min=1, max=gui_config.n_preview_samples).bind_value(iloc, 'value').classes('px-2 py-0')
                    ui.button(icon='refresh',on_click=next_batch).classes('p-3')

        sampleview.open()

    with infoview:
        with ui.dialog() as sampleview, ui.card().classes('items-center w-[1000px] h-[650px]').style('max-width: none; max-height: none'):
            sample_content = ui.column().classes('p-0 w-full items-center')

        with ui.row().classes('h-full items-center'):
            backbutton = ui.button(icon="arrow_back", on_click=lambda: show_subset_info(views, metadata, path)).classes('m-3')
            ui.label(subs).style("font-size:130%; font-weight:bold")
        with ui.column().classes('items-center p-5 gap-2 w-full'):
            ui.button("preview samples",icon="find_in_page", on_click= lambda:on_click_sample(-1)).classes('w-full')
        cols = [
            {'name': 'index', 'label': 'index', 'field': 'index', 'required': True, 'align': 'center', 'sortable': True},
            {'name': 'field', 'label': 'field', 'field': 'field', 'sortable': True, 'align': 'left'},
            {'name': 'dtype', 'label': 'dtype', 'field': 'dtype', 'sortable': True, 'align': 'left'},
        ]
        rows = [dict(index=i,field=field,dtype=str(dtype)) for i, (field, dtype) in enumerate(zip(schema.names, schema.types))]

        table = ui.table(columns=cols, rows=rows, 
                         row_key='index').classes('w-full py-3 px-5')
        table.on('rowClick', lambda e: on_click_sample(e.args[-2]['index']))


def show_subset_info(views, metadata, path):
    name, subs = path
    dataview, infoview = views["dataview"], views["infoview"]
    infoview.clear()
    def on_click_show(views, metadata, path):
        _path[1] = path[1]
        _search_value[0] = ""
        show_sesarch_bar(views, metadata)
        show_partitions(views, metadata)

    async def on_download():
        n = ui.notification(timeout=None)
        n.message = 'Downloading : See progress on the server'
        n.spinner = True
        await asyncio.sleep(0.1)
        async def coro():
            download(name, subset=subs, force_redownload=False)
        task = asyncio.create_task(coro())
        await task

        n.message = 'Done!'
        n.spinner = False
        metadata["datasets"]=load_meta()["datasets"]
        show_subsets(views, metadata)
        await asyncio.sleep(2)
        n.dismiss()

    async def on_remove():
        n = ui.notification(timeout=None)
        n.message = 'Removing data ...'
        n.spinner = True
        await asyncio.sleep(0.1)
        async def coro():
            remove(name, subset=subs, force_remove=True)
        task = asyncio.create_task(coro())
        await task

        n.message = 'Done!'
        n.spinner = False
        metadata["datasets"]=load_meta()["datasets"]
        show_subsets(views, metadata)
        await asyncio.sleep(2)
        n.dismiss()

    info = metadata["datasets"][name]["subsets"][subs]

    def on_click_schema():
        if info["downloaded"]<=0:
            ui.notify("Schema not available until at least one partition is downloaded.")
        else:
            show_schema(views, metadata, path)

    with infoview:
        with ui.dialog() as dialog, ui.card().classes('items-center'):
            ui.label('You are about to remove the downloaded data.')
            ui.markdown('**Are you sure?**')
            with ui.grid(columns=2).classes('w-full items-center'):
                ui.button('Yes', icon="delete", on_click=on_remove).classes('w-full').props(f'color=red')
                ui.button('No', on_click=dialog.close).classes('w-full')

        tab_names = ['Stream', 'Full']
        with ui.dialog() as snipet, ui.card().classes('items-center').style('width: 600px; height: 400px; max-width: none; max-height: none'):
            codemirrors = []
            with ui.tabs().classes('w-full') as tabs:
                codemirrors.append(ui.tab(tab_names[0]).classes('w-full'))
                codemirrors.append(ui.tab(tab_names[1]).classes('w-full'))
            with ui.tab_panels(tabs, value=tab_names[0]).classes('w-full'):
                with ui.tab_panel(tab_names[0]).classes('w-full'):
                    codemirrors[0] = ui.codemirror(
                        stream_load_code_snipet(name,subs),
                                              line_wrapping=True, language='Python').classes('h-full')
                with ui.tab_panel(tab_names[1]).classes('w-full'):
                    codemirrors[1] = ui.codemirror(
                        full_load_code_snipet(name,subs), 
                                              line_wrapping=True, language='Python').classes('h-full')

            def on_click():
                pyperclip.copy(codemirrors[tab_names.index(tabs.value)].value)
                ui.notify("Copied to clipboard")
                snipet.close()
            ui.button('Copy', 
                    on_click=on_click
                    ).classes('w-2/3').style('position: absolute; bottom: 10px;')

        with ui.column().classes("w-full items-center p-5 gap-2"):
            ui.button("Show partitions",icon="arrow_forward",on_click=lambda: on_click_show
            (views, metadata, path)).classes("w-full")
            if info["downloaded"]<len(info["partitions"]):
                ui.button("Download",icon="download", on_click=on_download).classes("w-full")
            if info["downloaded"]>0:
                ui.button("Remove data",icon="delete", on_click=dialog.open).classes("w-full").props(f'color=red')
            ui.button("Loader Snipet",icon="code", on_click=snipet.open).classes("w-full")
            ui.button("Show schema", icon="hub", on_click=on_click_schema).classes("w-full")
        show_subset_info_list(views, metadata, path)

def show_subsets(views, metadata, path=None):
    if path is None:
        name = _path
    else:
        _path[0] = path
    name = _path[0]
    dataview, infoview = views["dataview"], views["infoview"]

    dataview.clear()
    infoview.clear()
    columns, rows = subset_table(metadata["datasets"][name])
    def on_click_back(views, metadata):
        _path[0]=None
        _search_value[0] = ""
        show_sesarch_bar(views, metadata)
        show_datasets(views, metadata)
    with dataview:
        with ui.row():
            backbutton = ui.button(icon="arrow_back", on_click=lambda: on_click_back(views, metadata))
            ui.label(f"> {name}").style(_title_style)
        table = ui.table(columns=columns, rows=rows, 
                         row_key='name',
                         pagination={'rowsPerPage': 10, 'sortBy': 'downloaded', 'descending':True, 'page': 1}
                         ).classes('w-full')
        table.on('rowClick', lambda e: show_subset_info(views, metadata, (_path[0],e.args[-2]['name'])))

def show_dataset_info(views, metadata, name):
    dataview, infoview = views["dataview"], views["infoview"]

    infoview.clear()

    def on_click_show(views, metadata, name):
        _path[0] = name
        _search_value[0] = ""
        show_sesarch_bar(views, metadata)
        show_subsets(views, metadata)

    with infoview:
        with ui.row().classes("w-full items-center p-5"):
            ui.button("Show subsets",icon="arrow_forward",on_click=lambda: on_click_show(views, metadata, name)).classes("w-full")
        show_dataset_info_list(views, metadata, name)

def show_datasets(views, metadata):
    dataview, infoview = views["dataview"], views["infoview"]
    dataview.clear()
    infoview.clear()

    columns, rows = dataset_table(metadata)
    with dataview:
        table = ui.table(columns=columns, rows=rows, 
                            row_key='name',
                            pagination={'rowsPerPage': 10, 'sortBy': 'name', 'page': 1}
                            ).classes('w-full')
        table.on('rowClick', lambda e: show_dataset_info(views, metadata, e.args[-2]['name']))

def on_search(views, metadata, x):
    _search_value[0] = x
    if _path[0] is None:
        show_datasets(views, metadata)
    elif _path[1] is None:
        show_subsets(views, metadata)
    else:
        show_partitions(views, metadata)

def show_sesarch_bar(views, metadata):
    views["searchbar"].clear()
    with views["searchbar"]:
        ui.icon('search', color='primary').classes('items-center text-4xl h-full')
        ui.input(placeholder='Search', on_change=lambda x: on_search(views, metadata, x.value)).props('rounded outlined dense').classes('w-1/3 px-0')


def show(context):
    print("[INFO] reading metafiles")
    metadata = load_meta()
    reinit()
    context.clear()
    views = dict()
    with context:
        with ui.column().classes('bg-blue-100 no-wrap w-3/4 gap-0'):
            searchbar = ui.row().classes("w-full px-2 py-2 gap-0")
            views["searchbar"] = searchbar
            dataview = ui.column().classes('bg-blue-100 w-full px-2 py-1')
            views["dataview"] = dataview
        infoview = ui.column().classes('bg-blue-100 w-1/4')
        views["infoview"] = infoview
        show_datasets(views, metadata)
        show_sesarch_bar(views, metadata)
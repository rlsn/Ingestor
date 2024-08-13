import time

from nicegui import ui

columns = [
    {'name': 'name', 'label': 'Name', 'field': 'name', 'required': True},
    {'name': 'age', 'label': 'Age', 'field': 'age', 'sortable': True},
]
rows = [
    {'id': 0, 'name': 'Alice', 'age': 18},
    {'id': 1, 'name': 'Bob', 'age': 21},
    {'id': 2, 'name': 'Lionel', 'age': 19},
    {'id': 3, 'name': 'Michael', 'age': 32},
    {'id': 4, 'name': 'Julie', 'age': 12},
    {'id': 5, 'name': 'Livia', 'age': 25},
    {'id': 6, 'name': 'Carol'},
]

with ui.table(title='My Team', columns=columns, rows=rows, selection='multiple', pagination=10).classes('w-96') as table:
    with table.add_slot('top-right'):
        with ui.input(placeholder='Search').props('type=search').bind_value(table, 'filter').add_slot('append'):
            ui.icon('search')
    with table.add_slot('bottom-row'):
        with table.row():
            with table.cell():
                ui.button(on_click=lambda: (
                    table.add_rows({'id': time.time(), 'name': new_name.value, 'age': new_age.value}),
                    new_name.set_value(None),
                    new_age.set_value(None),
                ), icon='add').props('flat fab-mini')
            with table.cell():
                new_name = ui.input('Name')
            with table.cell():
                new_age = ui.number('Age')

ui.label().bind_text_from(table, 'selected', lambda val: f'Current selection: {val}')
ui.button('Remove', on_click=lambda: table.remove_rows(*table.selected)) \
    .bind_visibility_from(table, 'selected', backward=lambda val: bool(val))


ui.add_style('''
.my-sticky-table
  /* height or max-height is important */
  height: 310px

  /* specifying max-width so the example can
    highlight the sticky column on any browser window */
  max-width: 600px

  td:first-child
    /* bg color is important for td; just specify one */
    background-color: #00b4ff

  tr th
    position: sticky
    /* higher than z-index for td below */
    z-index: 2
    /* bg color is important; just specify one */
    background: #00b4ff

  /* this will be the loading indicator */
  thead tr:last-child th
    /* height of all previous header rows */
    top: 48px
    /* highest z-index */
    z-index: 3
  thead tr:first-child th
    top: 0
    z-index: 1
  tr:first-child th:first-child
    /* highest z-index */
    z-index: 3

  td:first-child
    z-index: 1

  td:first-child, th:first-child
    position: sticky
    left: 0

  /* prevent scrolling behind sticky top row on focus */
  tbody
    /* height of all previous header rows */
    scroll-margin-top: 48px
''', indented=True)

columns = [
    {'name': 'name', 'label': 'Name', 'field': 'name', 'align': 'left'},
    {'name': 'age', 'label': 'Age', 'field': 'age'},
    {'name': 'gender', 'label': 'Gender', 'field': 'gender'},
]
rows = [
    {'name': 'Alice', 'age': 18, 'gender': 'female'},
    {'name': 'Bob', 'age': 21, 'gender': 'male'},
    {'name': 'Carol', 'age': 42, 'gender': 'female'},
    {'name': 'Dave', 'age': 33, 'gender': 'male'},
]
ui.table(columns=columns, rows=rows, row_key='name').classes('my-sticky-table w-40 h-40')

ui.run()
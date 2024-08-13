from nicegui import ui

color_1 = '#E4EEFF'
color_2 = '#ABCBFF'
grey = '#D8D8D8'
ui.add_sass(f'''
.theme-color
  background-color: {color_1}
.row-selected
  background-color: {color_2}
.unavailable
  background-color: {grey}
''')


ui.add_scss(f'''
.sticky-header-table
  /* height or max-height is important */
  height: 750px

  .q-table__top,
  .q-table__bottom,
  thead tr:first-child th
    /* bg color is important for th; just specify one */
    background-color: {color_1}

  thead tr th
    position: sticky
    z-index: 1
  thead tr:first-child th
    top: 0

  /* this is when the loading indicator appears */
  &.q-table--loading thead tr:last-child th
    /* height of all previous header rows */
    top: 48px

  /* prevent scrolling behind sticky top row on focus */
  tbody
    /* height of all previous header rows */
    scroll-margin-top: 48px
''', indented=True)
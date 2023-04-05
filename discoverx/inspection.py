from copy import deepcopy
import numpy as np
import pandas as pd
from typing import Optional
from ipydatagrid import DataGrid, TextRenderer, VegaExpr
import ipywidgets as widgets
import IPython.display as ipython_display


class InspectionTool:
    def __init__(self, classification_pdf: pd.DataFrame, publish_function=None):
        self.publish_function = publish_function
        self.inspected_table: Optional[pd.DataFrame] = None

        # set up datagrid
        self.datagrid = self._setup_datagrid(classification_pdf, self._get_renderer())
        self.current_row = None

        # button to publish tags
        self.button = self._setup_button()
        self.text = widgets.HTML(value="<h2>Inspect Lakehouse Scan</h2>")

        # set up Tag inspection & modification part of UI
        self.tags_header = widgets.HTML(
            value="<h3>Inspect & Modify Tags </h3>\n<i>Select any row above to inspect current/existing tags, inspect those detected by the scan and inspect and modify the ones which are going to be published.</i>"
        )

        self.published_tags_header = widgets.HTML(
            value="<h4>Tags To Be Published (Edit + Press Enter)</h4>"
        )
        self.published_tags = widgets.Text(value="", disabled=False)
        self.published_tags.on_submit(self._on_publish_tags_submit)

        self.current_tags_header = widgets.HTML(value="<h4>Current Tags</h4>")
        self.current_tags = widgets.Text(value="", disabled=True)

        self.detected_tags_header = widgets.HTML(value="<h4>Detected Tags</h4>")
        self.detected_tags = widgets.Text(value="", disabled=True)

        # organize all elements below the datagrid
        self.headers = widgets.HBox(
            [
                self.current_tags_header,
                self.detected_tags_header,
                self.published_tags_header,
            ]
        )
        self.tag_text = widgets.HBox(
            [self.current_tags, self.detected_tags, self.published_tags]
        )
        gridbox = widgets.GridspecLayout(3, 3, layout=widgets.Layout(width="1100"))
        gridbox[0, 0] = self.current_tags_header
        gridbox[0, 1] = self.detected_tags_header
        gridbox[0, 2] = self.published_tags_header
        gridbox[1, 0] = self.current_tags
        gridbox[1, 1] = self.detected_tags
        gridbox[1, 2] = self.published_tags
        gridbox[2, 0] = self.button

        # complete UI layout
        self.inspection_widget = widgets.VBox(
            [self.text, self.datagrid, self.tags_header, gridbox]
        )

    def display(self):
        ipython_display.display_html(self.inspection_widget)

    def _setup_datagrid(self, pdf: pd.DataFrame, renderer):
        datagrid = DataGridAutoSize(
            pdf,
            base_row_size=30,
            base_column_size=150,
            default_renderer=renderer,
            selection_mode="row",
            auto_fit_columns=True,
            auto_fit_params={"area": "body", "padding": 50},
        )
        datagrid.on_cell_click(self._on_cell_clicked)
        return datagrid

    def _on_cell_clicked(self, cell):
        self.current_row = cell["primary_key_row"]
        self.published_tags.value = ", ".join(
            self.datagrid.get_cell_value(
                "Tags to be published", cell["primary_key_row"]
            )[0]
        )
        self.current_tags.value = ", ".join(
            self.datagrid.get_cell_value("Current Tags", cell["primary_key_row"])[0]
        )
        self.detected_tags.value = ", ".join(
            self.datagrid.get_cell_value("Detected Tags", cell["primary_key_row"])[0]
        )

    def _get_renderer(self):
        return TextRenderer(
            background_color=VegaExpr(
                "cell.metadata.data['Tags changed'][0] ? 'lightblue' :'white'"
            )
        )

    def _on_publish_tags_submit(self, text):
        if self.current_row is not None:
            new_input = [tag.replace(" ", "") for tag in text.value.split(",")]
            self.datagrid.set_cell_value(
                "Tags to be published", self.current_row, new_input
            )
            changed = self.datagrid.get_cell_value(
                "Current Tags", self.current_row
            ) != self.datagrid.get_cell_value("Tags to be published", self.current_row)
            self.datagrid.set_cell_value("Tags changed", self.current_row, changed)

    def _setup_button(self):
        button = widgets.Button(description="Publish All")
        button.on_click(self._on_button_clicked)
        return button

    def _on_button_clicked(self, b):
        self.inspected_table = self.datagrid.data
        self.datagrid.close()
        self.published_tags.disabled = True
        self.button.disabled = True
        self.button.button_style = "warning"
        self.button.description = "Publishing ..."
        self.publish_function(False)
        self.button.button_style = "success"
        self.button.description = "Published"
        self.set_button_checked()


# from https://github.com/bloomberg/ipydatagrid/issues/216
class DataGridAutoSize(DataGrid):
    _max_width = 576
    _max_height = 600
    _adjustment = 25

    def __init__(self, dataframe, **kwargs):

        if "index_name" in kwargs:
            self._index_name = kwargs["index_name"]
        else:
            self._index_name = None

        self._first_call = True
        self._layout_init = kwargs.get("layout", {})

        self.data = dataframe
        df = self.data
        kwargs = self.auto_resize_grid(df, **kwargs)
        self.test = kwargs

        super().__init__(dataframe, **kwargs)

    def auto_resize_grid(self, data, **kwargs):
        df = data
        max_width = kwargs.pop("max_width", self._max_width)
        max_height = kwargs.pop("max_height", self._max_height)
        adjustment = kwargs.pop("adjustment", self._adjustment)

        column_widths = kwargs.get("column_widths", {})
        base_row_size = kwargs.get("base_row_size", 20)
        base_column_size = kwargs.get("base_column_size", 64)
        base_row_header_size = kwargs.get("base_row_header_size", 64)
        base_column_header_size = kwargs.get("base_column_header_size", 20)
        index_names = [*df.index.names]
        index_size = np.sum(
            [column_widths.get(v, base_row_header_size) for v in index_names]
        )
        columns_size = np.sum(
            [column_widths.get(v, base_column_size) for v in df.columns]
        )

        width = index_size + columns_size + adjustment
        height = (
            len(df) * base_row_size
            + df.columns.nlevels * base_column_header_size
            + adjustment
        )
        layout = kwargs.pop("layout", {})
        lo_width = layout.get("width", f"{width if width < max_width else ''}px")
        lo_height = layout.get("height", f"{height if height < max_height else ''}px")
        if lo_width != "px":
            layout["width"] = lo_width
        if lo_height != "px":
            layout["height"] = lo_height
        layout["align-content"] = "center"
        kwargs["layout"] = layout
        return kwargs

    @DataGrid.data.setter
    def data(self, dataframe):
        # Reference for the original frame column and index names
        # This is used to when returning the view data model
        self.__dataframe_reference_index_names = dataframe.index.names
        self.__dataframe_reference_columns = dataframe.columns
        dataframe = dataframe.copy()

        # Primary key used
        index_key = self.get_dataframe_index(dataframe)

        self._data = self.generate_data_object(dataframe, "ipydguuid", index_key)

        if not self._first_call:
            kwargs = self.auto_resize_grid(
                self.data,
                column_widths=self.column_widths,
                base_row_size=self.base_row_size,
                base_column_size=self.base_column_size,
                base_row_header_size=self.base_row_header_size,
                base_column_header_size=self.base_column_header_size,
                layout=deepcopy(self._layout_init),
            )

            self.layout = kwargs["layout"]
        else:
            self._first_call = False
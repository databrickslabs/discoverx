import pandas as pd
from ipydatagrid import DataGrid, TextRenderer, VegaExpr
import ipywidgets as widgets
import IPython.display as ipython_display

class InspectionTool:
  def __init__(self, staged_updates_pdf: pd.DataFrame):
    self.staged_updates_pdf = staged_updates_pdf
    self.datagrid = self._setup_datagrid(self._get_renderer())
    self.button = self._setup_button()
    self.text = self._setup_text()
    self.inspection_widget = widgets.VBox([self.text, self.datagrid, self.button])


  def display(self):
    ipython_display.display_html(self.inspection_widget)
  
  def _setup_datagrid(self, renderer):
    datagrid = DataGrid(self.staged_updates_pdf, base_row_size=30, base_column_size=150, default_renderer=renderer)
    datagrid.on_cell_click(self._on_cell_clicked)
    return datagrid

  def _on_cell_clicked(self, cell):
    if self.datagrid.get_cell_value("action", cell["primary_key_row"]) != ["unset"]:
      if cell["column"] == "tag_status":
        if cell["cell_value"] == "active":
          self.datagrid.set_cell_value(cell["column"], cell["primary_key_row"], "inactive")
        else:
          self.datagrid.set_cell_value(cell["column"], cell["primary_key_row"], "active")
        if self.datagrid.get_cell_value("tag_status", cell["primary_key_row"])[0] != self.staged_updates_pdf["tag_status"].iloc[cell["primary_key_row"]]:
          self.datagrid.set_cell_value("action", cell["primary_key_row"], "set")
        else:
          self.datagrid.set_cell_value("action", cell["primary_key_row"], self.staged_updates_pdf["action"].iloc[cell["primary_key_row"]])

  def _get_renderer(self):
    return TextRenderer(
      background_color=VegaExpr(
          "cell.metadata.data['action'] == 'set' ? 'lightblue' : cell.metadata.data['action'] == 'unset' ? 'red' : 'white'"
      )
    )

  def _setup_button(self):
    button = widgets.Button(description="Apply")
    button.on_click(self._on_button_clicked)
    return button

  def _on_button_clicked(self, b):
    self.staged_updates_pdf = self.datagrid.data
    self.button.icon = "check"
    self.datagrid.close()
    self.button.disabled = True

  @staticmethod
  def _setup_text():
    return widgets.HTML(value="""
    <h1>Inspect Lakehouse Scan</h1>
<ul>
  <li>The following table presents the results of your Lakehouse scan/classification/tagging and shows which tags will be added, kept or removed.</li>
  <li>By clicking on a specific column header you can choose to sort or filter for specific column values.</li>
  <li>The <b>tag_status</b> shows which tags have been manually removed (inactive). You can validate the current status and manually change by clicking on the corresponding tag_status cell.</li>
  <li>Click the 'Apply' button once done with validation. If any changes were done this will update the classification.</li>
  <li>Call <b>dx.save_tags()</b> to publish the classification and set tags in Unity Catalog</li>
</ul>
  """
)


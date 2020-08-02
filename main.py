""" A graphical client for Elasticsearch that returns results in the form of a Pandas DataFrame.
Results can be manipulated via a built-in terminal session, or exported as an Excel file. """
import code
import http.client as http
import json
import sys
import tkinter as tk
import tkinter.ttk as ttk
import tkinter.filedialog as tkf
import pandas as pd

# Functions that interact with Elasticsearch. They are written when possible in a functional style,
# meaning that they simply receive input and return the proper output, and do not depend on the
# state of any GUI elements.

# TODO: Implement HTTPS connections. Right now, the protocol variable exists but is unused

def request_builder(search_term, fields: str) -> str:
    """ Creates the JSON search request body from various form elements.
    This function will grow as more form options are added. """
    body = dict()
    query = dict()
    query['multi_match'] = {"query": search_term}
    body['_source'] = list(fields.split(','))
    # Static value of 100 as a sane default, will create widget with 0-10000 hit range
    body['size'] = 100
    body['query'] = query
    return json.dumps(body)

def index_listing(protocol: bool, ip, port: str) -> list:
    """ Retrive a list of indices from an Elastic server """
    fields = http.HTTPConnection(ip + ':' + port)
    fields.request(method="GET", url='/_alias', encode_chunked=False)
    requestjson = json.loads(fields.getresponse().read())
    fields.close()
    return list(requestjson)

def index_field_names(protocol: bool, ip, port, index: str) -> list:
    """ Retrive mappings (field names) for documents in selected indices """
    fields = http.HTTPConnection(ip + ':' + port)
    fields.request(method="GET", url='/' + index + '/_mapping', encode_chunked=False)
    requestjson = json.loads(fields.getresponse().read())
    fields.close()
    fieldlist = list()
    for key in list(requestjson.keys()):
        mappings = requestjson.get(key).get('mappings').get('properties')
        # This will eventually need to be re-written in a recursive manner, as
        # nested fields can go up to 20 levels deep by default in Elastic
        for f in mappings:
            if 'properties' in mappings.get(f).keys():
                for sf in mappings.get(f).get('properties').keys():
                    fieldlist.append(f + '.' + sf)
            else:
                fieldlist.append(f)
    return fieldlist

def simple_query_search(protocol: bool, ip, port, index, search_term, fields: str) -> pd.DataFrame:
    """ Simple query match that should hit on any field in a document """
    search = http.HTTPConnection(ip + ':' + port)
    search.request(method="GET", url='/' + index + '/_search',
                   body=request_builder(search_term, fields),
                   headers={"Content-Type": "application/json"}, encode_chunked=False)
    requestjson = json.loads(search.getresponse().read())
    search.close()
    results = pd.DataFrame()
    for hit in requestjson.pop('hits').pop('hits'):
        hitdict = dict()
        # This will eventually need to be re-written in a recursive manner, as
        # nested fields can go up to 20 levels deep by default in Elastic. Nested
        # fields need to be unpacked, though, so that the pandas quicksort functions
        # can actually work as intended
        for (f, v) in hit.pop('_source').items():
            if isinstance(v, dict):
                for (sf, sf_v) in v.items():
                    if isinstance(sf_v, dict):
                        for (ssf, ssf_v) in sf_v.items():
                            hitdict[f + '.' + sf + '.' + ssf] = ssf_v
                    else:
                        hitdict[f + '.' + sf] = sf_v
            else:
                hitdict[f] = v
        results = results.append(hitdict, ignore_index=True)
    return results

# GUI elements below; perhaps split this into a separate .py file at some point

# Probably need to re-factor at some point and make some of these nested classes. For instance,
# PandasSession assumes that it was invoked by ResultsGrid. Although that will always be true
# for this program, it hurts reusability.

class PandasSession(tk.Toplevel):
    """ Psuedo-console session with current search results pre-loaded."""
    def __init__(self, master=None):
        super().__init__(master)
        self.master = master
        # Catch window destruction and reroute to custom function
        self.protocol("WM_DELETE_WINDOW", lambda: self.delete_window())
        self.create_widgets()
        self.place_widgets()

    class OutputWindow(tk.Text):
        """ This is a stripped-down version of the OutputWindow class found in
        the idlelib.outwin module from the Python standard libraries. It focuses on
        simply writing stdout and stderr to the widget """

        def write(self, s, tags=(), mark="insert"):
            """ Writes text to text widget. """
            assert isinstance(s, str)
            self.insert(mark, s, tags)
            self.see(mark)
            self.update()
            return len(s)

        def writelines(self, lines):
            """ Write each item in lines iterable. """
            for line in lines:
                self.write(line)

        def flush(self):
            """ No flushing needed as write() directly writes to widget. """
            pass

    def create_widgets(self):
        """ Create widgets within frame """
        self.output = self.OutputWindow(self)
        sys.stdout = self.output
        sys.stderr = self.output
        self.xscroll = ttk.Scrollbar(self, orient="horizontal")
        self.yscroll = ttk.Scrollbar(self, orient="vertical")
        self.output.configure(xscrollcommand=self.xscroll.set, yscrollcommand=self.yscroll.set)
        self.xscroll.configure(command=self.output.xview)
        self.yscroll.configure(command=self.output.yview)
        self.entry = ttk.Entry(self)
        self.entry.bind(sequence='<Return>', func=lambda send: self.send_input())

    def place_widgets(self):
        """ Place widgets within frame """
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)
        self.output.grid(row=0, column=0, sticky="nwes", columnspan=2)
        self.xscroll.grid(row=1, column=0, sticky="we", columnspan=2)
        self.yscroll.grid(row=0, column=2, sticky="ns")
        self.entry.grid(row=2, column=0, sticky="we", columnspan=3)

    def send_input(self):
        """ Send contents of text entry to console after Enter is pressed.
        Catches exit() and quit(), as they would close the entire program
        if invoked """
        self.input = self.entry.get()
        if self.input != ('exit()' or 'quit()'):
            self.console.push(self.entry.get())
            self.console.resetbuffer()
            self.entry.delete(first=0, last="end")

    def init_session(self, results: dict):
        """ Creates a console session, loads the search results DataFrame that is
        passed to it, bound to 'r', and loads numpy/pandas/pyplot with commonly
        used aliases """
        self.console = code.InteractiveConsole(locals=results)
        self.console.push('import numpy as np')
        self.console.push('import pandas as pd')
        self.console.push('import matplotlib.pyplot as plt')
        # Trying to print a multiline string with an already escaped print statement
        # got weird. I'll see if there's a better way to do the lines below
        self.console.push('print(\'Search results imported as r\')')
        self.console.push('print(\'Numpy imported as np\')')
        self.console.push('print(\'Pandas imported as pd\')')
        self.console.push('print(\'Pyplot imported as plt\')')
        self.console.push('print(\'Happy hunting!\')')
        self.mainloop()

    def delete_window(self):
        """ Restores search button functionality and refreshes tree view of results
        before closing window """
        self.master.master.simple_search.state(['!disabled'])
        self.master.pandas_session.state(['!disabled'])
        self.master.export_search.state(['!disabled'])
        self.master.populate()
        self.destroy()

class LabeledEntry(ttk.Frame):
    """ Frame grouping a text entry field with its caption for easier geometry management """
    def __init__(self, master=None):
        super().__init__(master)
        self.master = master
        self.create_widgets()
        self.place_widgets()

    def create_widgets(self):
        """ Create widgets within frame """
        self.label = ttk.Label(self)
        self.entry = ttk.Entry(self)

    def place_widgets(self):
        """ Place widgets within frame, should appear as label on top with entry box under it """
        self.label.pack(anchor="c", expand=0)
        self.entry.pack(anchor="c", after=self.label, expand=1)

class ScrollingChecklist(ttk.Frame):
    """ Scrollable checkbox list, along with some useful functions """
    def __init__(self, master=None):
        super().__init__(master)
        self.master = master
        self.create_widgets()
        self.place_widgets()
        self.configure(style='White.TFrame', borderwidth=1, relief="sunken")

    def create_widgets(self):
        """ Put checklist widgets here """
        self.canvas = tk.Canvas(self, background="white", highlightthickness=0)
        self.yscroll = ttk.Scrollbar(self, orient="vertical")
        self.canvas.configure(yscrollcommand=self.yscroll.set)
        self.yscroll.configure(command=self.canvas.yview)
        self.checklist = ttk.Frame(self.canvas, style='White.TFrame')
        self.canvas.create_window((0, 0), window=self.checklist, anchor="nw")

    def place_widgets(self):
        """ Place widgets side-by-side """
        self.canvas.grid(row=0, column=0, sticky="nw")
        self.yscroll.grid(row=0, column=1, sticky="ns")

    def uncheck_rest(self):
        """ Used by the 'All' option to clear other checklist entries, assuming 'All' is first  """
        if 'selected' in self.checklist.slaves()[0].state():
            for checkbox in self.checklist.slaves()[1:]:
                checkbox.state(['!selected'])

    def return_checked(self) -> str:
        """ Returns a comma separated string of the names for checkboxes that are selected """
        checked = list()
        for checkbox in self.checklist.slaves()[1:]:
            if 'selected' in checkbox.state():
                checked.append(checkbox.cget("text"))
        return ",".join(checked)

    def set_scroll_area(self):
        """ Updates scrollable height of widget after adding/removing items """
        self.update()
        self.canvas.configure(scrollregion=(0, 0, 0, self.checklist.winfo_reqheight()))

    def clear(self):
        """ Removes all checkboxes from checklist """
        for checkbox in self.checklist.slaves():
            checkbox.destroy()

class ResultsGrid(ttk.Frame):
    """ A table view of the search results, with some DataFrame-related functions """

    def __init__(self, master=None):
        super().__init__(master)
        self.master = master
        self.create_widgets()
        self.place_widgets()
        self.search_results = pd.DataFrame()

    def create_widgets(self):
        """ Create widgets within frame """
        self.treeview = ttk.Treeview(self, show="headings")
        self.xscroll = ttk.Scrollbar(self, orient="horizontal")
        self.yscroll = ttk.Scrollbar(self, orient="vertical")
        self.treeview.configure(xscrollcommand=self.xscroll.set, yscrollcommand=self.yscroll.set)
        self.xscroll.configure(command=self.treeview.xview)
        self.yscroll.configure(command=self.treeview.yview)
        self.export_search = ttk.Button(self, text="Save as Excel File",
                                        command=lambda: self.search_results.to_excel(
                                            tkf.asksaveasfilename(defaultextension='.xlsx',
                                                                  filetypes=[('Excel', '*.xlsx')]),
                                            engine='openpyxl'))
        self.pandas_session = ttk.Button(self, text="Open Results with Pandas",
                                         command=lambda: self.console_session())

    def place_widgets(self):
        """ Place widgets within frame, should appear as label on top with entry box under it """
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)
        self.treeview.grid(row=0, column=0, sticky="nwes", columnspan=2)
        self.xscroll.grid(row=1, column=0, sticky="we", columnspan=2)
        self.yscroll.grid(row=0, column=2, sticky="ns")
        self.export_search.grid(row=2, column=1, sticky="e")
        self.pandas_session.grid(row=2, column=0, sticky="w")

    def populate(self):
        """ Populate the grid view with the current contents of the search results dataframe """
        self.treeview.delete(*self.treeview.get_children()) # Clear old contents
        search_results_columns = self.search_results.columns.values.tolist()
        self.treeview.configure(columns=search_results_columns)
        for header in search_results_columns:
            self.treeview.heading(header, text=header,
                                  command=lambda _header=header: self.column_sort_asc(_header))
        for result in self.search_results.values:
            self.treeview.insert(parent="", index="end", values=result.tolist())

    def column_sort_asc(self, header: str):
        """ Do a dataframe sort by whichever column is clicked """
        self.search_results.sort_values(by=header, kind="mergesort",
                                        inplace=True, ignore_index=True)
        self.populate()
        self.treeview.heading(header,
                              command=lambda _header=header: self.column_sort_desc(_header))

    def column_sort_desc(self, header: str):
        """ Do a dataframe sort by whichever column from the grid is clicked """
        self.search_results.sort_values(by=header, kind="mergesort",
                                        ascending=False, inplace=True, ignore_index=True)
        self.populate()
        self.treeview.heading(header, command=lambda _header=header: self.column_sort_asc(_header))

    def console_session(self):
        """ Spawns console window and binds search results to 'r' """
        self.master.simple_search.state(['disabled'])
        self.export_search.state(['disabled'])
        self.pandas_session.state(['disabled'])
        console = PandasSession(self)
        console.init_session({'r': self.search_results})

class SearchBuilder(ttk.Frame):
    """ The Elastic search builder; essentially the main window of the program """
    def __init__(self, master=None):
        super().__init__(master)
        self.master = master
        self.pack(expand=True, fill="both")
        self.set_tkvars()
        self.create_widgets()
        self.place_widgets()

    def set_tkvars(self):
        """ Create variable to store http/https checkbox preference """
        self.protocol = tk.BooleanVar(self)
        self.protocol.set(False)

    def create_widgets(self):
        """ Create widgets """
        self.input = ttk.Frame(self)
        self.ip = LabeledEntry(self.input)
        self.ip.label.configure(text="IP/Hostname")
        self.port = LabeledEntry(self.input)
        self.port.label.configure(text="Port")
        self.protocol_selector = ttk.Checkbutton(self.input, variable=self.protocol,
                                                 offvalue=False, onvalue=True,
                                                 text="HTTPS?")
        self.get_indices = ttk.Button(self.input, text="Get Indices",
                                      command=lambda: self.get_index_names())
        self.indices = ScrollingChecklist(self.input)
        self.indices.configure(style="ScrollingChecklist.TFrame")
        self.get_fields = ttk.Button(self.input, text="Get Field Names",
                                     command=lambda: self.get_field_names(), state="disabled")
        self.fields = ScrollingChecklist(self.input)
        self.simple_search_entry = ttk.Entry(self.input)
        self.simple_search = ttk.Button(self.input, text="Simple Query Search",
                                        command=lambda: self.simple_query_search())
        self.output = ResultsGrid(self)

    def place_widgets(self):
        """ Place widgets """
        self.ip.pack()
        self.port.pack()
        self.protocol_selector.pack()
        self.get_indices.pack()
        self.indices.pack()
        self.get_fields.pack()
        self.fields.pack()
        self.simple_search_entry.pack()
        self.simple_search.pack()
        self.input.pack(side="left", anchor="nw")
        self.output.pack(side="right", anchor="nw", expand=True, fill="both")

    def get_index_names(self):
        """ Populates index list checkboxes once connected to Elastic """
        self.indices.clear()
        self.all = ttk.Checkbutton(self.indices.checklist, text="All",
                                   command=lambda: self.indices.uncheck_rest(),
                                   style='White.TCheckbutton')
        self.all.state(['!alternate'])
        self.all.pack(anchor="nw")
        for entry in index_listing(self.protocol.get(), self.ip.entry.get(), self.port.entry.get()):
            if not entry.startswith('.'): # This should filter out Elastic's internal indices
                self.entry = ttk.Checkbutton(self.indices.checklist, text=entry,
                                             command=lambda: self.all.state(['!selected']),
                                             style='White.TCheckbutton')
                self.entry.pack(anchor="nw")
                self.entry.state(['!alternate'])
        self.get_fields.configure(state="active")
        self.indices.set_scroll_area()

    def get_field_names(self):
        """ Sets output area to display field names for an index """
        self.fields.clear()
        self.all = ttk.Checkbutton(self.fields.checklist, text="All",
                                   command=lambda: self.fields.uncheck_rest(),
                                   style='White.TCheckbutton')
        self.all.pack(anchor="nw")
        self.all.state(['!alternate'])
        maplist = index_field_names(self.protocol.get(), self.ip.entry.get(),
                                    self.port.entry.get(), self.indices.return_checked())
        for entry in maplist:
            if entry.startswith('@'): # Python doesn't like @ as part of a variable name
                at_entry = ('internal_' + entry.strip('@'))
                self.at_entry = ttk.Checkbutton(self.fields.checklist, text=entry,
                                                command=lambda: self.all.state(['!selected']),
                                                style='White.TCheckbutton')
                self.at_entry.pack(anchor="nw")
                self.at_entry.state(['!alternate'])
            else:
                self.entry = ttk.Checkbutton(self.fields.checklist, text=entry,
                                             command=lambda: self.all.state(['!selected']),
                                             style='White.TCheckbutton')
                self.entry.pack(anchor="nw")
                self.entry.state(['!alternate'])
        self.fields.set_scroll_area()

    def simple_query_search(self):
        """ Sets output area to display search results """
        if 'selected' in self.indices.checklist.slaves()[0].state():
            selected_indices = "_all"
        else:
            selected_indices = self.indices.return_checked()
        if 'selected' in self.fields.checklist.slaves()[0].state():
            selected_fields = "*"
        else:
            selected_fields = self.fields.return_checked()
        self.output.search_results = simple_query_search(self.protocol.get(), self.ip.entry.get(),
                                                         self.port.entry.get(), selected_indices,
                                                         self.simple_search_entry.get(),
                                                         selected_fields)
        self.output.populate()

# Instantiate Tk interpreter and main window
root = tk.Tk()
root.title("Pylastic ALPHA")
root.resizable(True, True)

# Define custom styles used by certain widgets
style = ttk.Style()
style.configure('White.TCheckbutton', background="white")
style.configure('White.TFrame', background="white")

# Populate main window and start program
pylastic = SearchBuilder(master=root)
pylastic.mainloop()

def create_base_widgets(wm):
    wm.create("container_name", "raw")
    wm.create("file_pattern", "*.csv")
    wm.create("file_encoding", "utf-8")
    wm.create("file_delimiter", ",")
    wm.create("file_quotechar", '"')
    wm.create("file_escapechar", "\\")
    wm.create("skip_lines", "0")
    wm.create("audit_table", "default.audit_log")
    wm.create("sheet_name", "Sheet1")
    wm.create("start_cell", "A1")
#Test

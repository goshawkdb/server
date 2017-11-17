# Debugger

If you compile goshawkdb with:

    go build -tags debug

then when you run it, it will put out a lot of info over `stderr`. So
much so that then working with those logs because hard for normal
editors. So this is a specific tool to process these logs.

To gather logs, it's recommended you redirect `stderr` straight to a
file:

    goshawkdb ...normal flags... 2> /path/to/file.log

then, open the logs with this debug tool:

    ./debug /path/to/file.log

You probably want to maximise your terminal window.

## Key bindings

* Press 'q' to quit.
* Left and right will change the selected column.
* Cursor keys up and down will move you up and down through the log
  file. PageUp and PageDown will move you quickly up and down (about
  half a screen per press).

## Limiting and searching rows

Initially, all rows are shown. But there can be a lot of rows so it's
useful to be able to limit the number of rows available. Navigate to a
value you wish to limit on, and press 'l'. Now the only rows that are
available will be rows that have the same value for that column. This
feature is inspired by mutt's limit function.

* Press 'a' to reset the limit to all rows.
* Using 'l' multiple times will further limit the available rows. So
  you may need to use 'a' to reset the limit, and then use 'l' to
  change the limit.
* The debugger keeps the cursor on the same row and column as you
  change the limited rows.

Instead of limiting rows, sometimes you want to search for matching
rows, but keeping the surrounding context (non-matching
rows). Navigate as before to a value you wish to search for, and then
press 'r' or 's'. The limited rows are not altered, but rows with
matching values for the selected column are highlighted. The design of
this feature is broadly based on emacs's interactive search (think C-s
C-w).

* 's' will jump to the next matching row.
* 'r' will jump to the previous matching row.
* Press return to clear the search.
* The search works within the limited rows, so if you want to search
  all rows, don't forget to press 'a' first.

## Hiding and showing columns

The default ordering of the columns is based on frequency analysis of
how many rows have entries for that column. So columns which are used
by more rows will be on the left of columns which are used by fewer
rows. Columns which are used by equally many rows are sorted
alphabetically by name. Thus the initial layout of colums should be
both deterministic and broadly useful.

Pressing 'c' brings up a dialogue to select which columns you wish to
see.

* To close the dialogue, press 'c' again.
* At this point the numbers '0' to '9' will directly toggle on and off
  the first 10 columns.
* Otherwise, use cursors up and down to move to the column you want,
  and then space to toggle it off and on.
* Pressing 'a' will turn on all columns.
* Pressing 'h' will turn off all columns which have no values for the
  currently limited rows.
* Pressing PageUp and PageDown will move the current column up or down
  in the column list.
* You cannot turn off all columns. That would be silly.

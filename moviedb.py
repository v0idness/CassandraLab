import cql

con = cql.connect(54.185.30.189, 9160, group6)
cursor = con.cursor()
#cursor.execute("CQL QUERY", {kw=Foo, kw2=Bar, etc...})

    # cursor.description  # None initially, list of N tuples that represent
    #                           the N columns in a row after an execute. Only
    #                           contains type and name info, not values.
    # cursor.rowcount     # -1 initially, N after an execute
    # cursor.arraysize    # variable size of a fetchmany call
    # cursor.fetchone()   # returns  a single row
    # cursor.fetchmany()  # returns  self.arraysize # of rows
    # cursor.fetchall()   # returns  all rows, don't do this.

#cursor.execute("ANOTHER QUERY", **more_kwargs)
#for row in cursor:  # Iteration is equivalent to lots of fetchone() calls
#    doRowMagic(row)

cursor.close()
con.close()
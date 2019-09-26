import pyexcel as p
records = p.iget_records(file_name="nome.xlsx")
for record in records:
    print("%s is aged at %d" % (record['Name'], record['Age']))
p.free_resources()
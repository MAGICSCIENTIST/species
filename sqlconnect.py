import sqlite3
import os

def _fortator_replace(value, formator):

    return value.replace(formator[1], formator[2]) if value is not None else None

def _formateValue(value, formatorList):
    _value = value
    for formator in formatorList:
        formatorType = formator[0]
        if(formatorType == "replace"):
            _value = _fortator_replace(_value, formator)
        
    return _value    

class database:
    def connect(self, db_file):
        """ create a database connection to a SQLite database """
        conn = None
        try:
            conn = sqlite3.connect(db_file)
        except sqlite3.Error as e:
            print(e)
        return conn


    def createTable(self, conn, tableName, fields):
        # fields = [
        #     {name: "id", type: "INTEGER", primary: True,},
        #     {name: "name", type: "TEXT"},
        # ]

        fields
        tableSchema = ", ".join(["{0} {1} {2}".format(field['fieldName'], field['type'], "PRIMARY KEY" if ("isPK" in field.keys() and field['isPK']) else "") for field in fields])

        sql = "CREATE TABLE IF NOT EXISTS {0} ({1})".format(tableName, tableSchema)
        try:
            c = conn.cursor()
            c.execute(sql)
        except sqlite3.Error as e:
            print(e)

    def convertToBinaryData(self, filename):
        # Convert digital data to binary format
        with open(filename, 'rb') as file:
            blobData = file.read()
        return blobData

    def insertData(self, conn, tbName, dataObj, fields):
        # struct = {
        #     "id": 1,
        #     "name": "test",
        # }


        sql = "INSERT INTO {0} ({1}) VALUES ({2})".format(tbName, ", ".join([o["fieldName"] for o in fields]), ", ".join(["?" for i in range(len(fields))]))
        tlist = []
        for field in fields:
            propertyPath = "propertyPath" in field.keys() and field["propertyPath"] or [field["fieldName"]]
            formator = "formator" in field.keys() and field["formator"] or None
            if propertyPath:
                value = dataObj
                for p in propertyPath:
                    if(isinstance(value, str) or p not in value.keys()):
                        value = None
                        break
                    else:
                        value = value[p]
                if formator:
                    value = _formateValue(value, formator)
                tlist.append(value)            
        data_tuple = tuple(tlist)

        try:
            c = conn.cursor()
            c.execute(sql, data_tuple)
            # conn.commit()
        except sqlite3.Error as e:
            print(e)
# def main():
#     db_file = "./result/result.db"
#     isRegenerate = False
#     if isRegenerate and os.path.exists(db_file):
#         os.remove(db_file)

#     with connect(db_file) as conn:
#         pass
    


# if __name__ == '__main__':
#     main()
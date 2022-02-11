import json
import jsonschema
from jsonschema import validate

# from csvvalidator import CSVValidator
class Json_Validation:

	def validate_json(conf_schema, input_):
		try:
			validate(instance=input_, schema=conf_schema)
		except jsonschema.exceptions.ValidationError as e:
			print(e)
			return False
		except Exception as e:
			print(e)
			return False
		return True

	def validateJSON_syntax(jsonData):
		try:
			json.loads(jsonData)
		except ValueError as err:
			return False
		return True

class CSV_Validation:

	def validate_csv(df,file_data):
		try:
			json_data = json.loads(file_data)
			#file_name = json_data["header"]["file_name"]
			column_names = json_data["header"]["column_names"]
			no_of_columns = json_data["header"]["no_of_columns"]
			if (set(df.columns.values) == set(column_names)) and (len(df.columns.values) == no_of_columns):
				#print(df.columns.values)
				#print("column_names", column_names)
				#print("no_of_columns", no_of_columns)
				return True
			else:
				return False

		except Exception as e:
			print(e)
			return False



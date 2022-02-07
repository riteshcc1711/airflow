conf_schema = {
  "type": "object",
  "properties": {
    "inputs":{
        "type":"array",
        "items":{"$ref":"#/$defs/groups"}
    },

  },
  "$defs": {
    "groups": {
      "type": "object",
      "required": [ "name", "value"],
      "properties": {
        "name": {
          "type": "integer",
        },
        "value": {
          "type": ["integer", "string"]
        },
  "required": [
        "inputs",
    ]
}}}}


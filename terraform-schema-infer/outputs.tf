output "schemas" {
  description = "Map of topic name to inferred schema string"
  value       = { for topic, result in data.external.schema : topic => result.result.schema }
}

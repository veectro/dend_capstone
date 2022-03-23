variable "cluster_identifier" {
  type = string
}

variable "db_name" {
  type = string
}

variable "username" {
  type = string
}

variable "port" {
  type    = number
  default = 5439
}

variable "node_type" {
  type    = string
  default = "dc2.large"
}

variable "cluster_type" {
  type    = string
  default = "single-node"
}

variable "number_of_nodes" {
  type    = number
  default = 4
}

variable "iam_roles" {
  type    = list(string)
}

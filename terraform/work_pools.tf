resource "prefect_work_pool" "general" {
  name = "pool-general"
  type = "process"
}

resource "prefect_work_pool" "ocr" {
  name = "pool-ocr"
  type = "process"
}

resource "prefect_work_pool" "crawl" {
  name = "pool-crawl"
  type = "process"
}

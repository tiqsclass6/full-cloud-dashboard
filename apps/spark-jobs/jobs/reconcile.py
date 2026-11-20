"""Entry point for `spark-submit` (the SparkApplication's mainApplicationFile)."""
from lakehouse_jobs.reconcile import main

if __name__ == "__main__":
    main()

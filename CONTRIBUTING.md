# **Contributing**

## **Workflow**

1. Branch from `main`; one topic per branch.
2. Make the change with its test, and update the page that documents it (results pages, runbooks, ADRs for decisions).
3. Run the checks for what you touched (below). There is no CI pipeline yet, so these are the gate.
4. Open a pull request that says what changed, how it was verified, and what was not verified.

Commits are signed (GPG). Never commit secrets: credentials live in Kubernetes Secrets, AWS Secrets Manager or `local/secrets/` (ignored); the AWS account id stays out of the repository.

## **Checks**

| **You changed**                        | **Run**                                                                                          |
| -------------------------------------- | ------------------------------------------------------------------------------------------------ |
| **Helm charts or values**              | `make helm-check`, then `make gitops-push` and `make gitops-status` on kind                      |
| **Java services**                      | `./gradlew test` in the app directory (Testcontainers needs Docker)                              |
| **Spark jobs**                         | `make local-spark-test`                                                                          |
| **Agent, MCP tools**                   | `make ai-test`; with the cluster, `make ai-ops-test`                                             |
| **Alert rules or dashboards**          | `make rules-test`; `make dashboards` (generated, do not edit the JSON by hand)                   |
| **Kyverno policies, network policies** | `make security-test`                                                                             |
| **Images or dependencies**             | `make security-scan`; Python dependencies are re-locked with `make ai-lock`                      |
| **Terraform**                          | `make tf-fmt tf-validate`; `make aws-plan` and read the plan                                     |
| **Markdown**                           | `make docs-format` (aligned tables); update the diagram in `docs/diagrams/` if its facts changed |

## **Conventions**

- **Pin versions**, and only adopt releases that are at least two weeks old.
- **Report honestly.** A single run is reported as one run, with what it does and does not prove. Keep the "what went wrong" notes; they are the most useful part of a results page.
- **Everything through Git.** The cluster is reconciled by Argo CD; a manual `kubectl` change is either an experiment (pause sync first) or a bug.
- **AWS is ephemeral.** Apply, verify, destroy; a person confirms every `terraform apply`.

See also the [**code of conduct**](CODE_OF_CONDUCT.md) and the [**security policy**](SECURITY.md).

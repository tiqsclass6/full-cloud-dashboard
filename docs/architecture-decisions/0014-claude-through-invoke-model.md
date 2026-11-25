# **ADR 0014: Claude through InvokeModel and the US inference profiles**

- Status: accepted
- Date: 2026-11-25

## **Context**
[**ADR 0010**](0010-agentic-ops.md) had the agent call the Messages API on the `bedrock-mantle` endpoint. Until 2026-11-25 the account had no agreement for any Claude model, so that path was never answered by a model.

On 2026-11-25 the agreement for Claude Sonnet 4.6 showed `AVAILABLE` (every other Claude model, Opus 5.5 included, still `NOT_AVAILABLE`). With the agent's own kind role:

| **Call**                                                  | **Answer**                                           |
| --------------------------------------------------------- | ---------------------------------------------------- |
| **Mantle, `anthropic.claude-sonnet-4-6`**                 | 404: the model does not exist                        |
| **Mantle, `us.anthropic.claude-sonnet-4-6`**              | 404: the model does not exist                        |
| **Mantle, `anthropic.claude-opus-5-5`**                   | 403: not available for this account                  |
| **InvokeModel, profile `us.anthropic.claude-sonnet-4-6`** | Answered; the agent's full request shape also worked |

## **Decisions**
1. **The agent calls Claude through InvokeModel** (the Anthropic SDK's `AnthropicBedrock` client) with the US cross-region inference profiles: `us.anthropic.claude-sonnet-4-6`, and `us.anthropic.claude-opus-5-5` as the refusal fallback. The request body is unchanged: adaptive thinking, effort `medium`, the JSON-schema `Diagnosis`, cache breakpoints.
2. **IAM allows InvokeModel on the two profiles, and the two models only through them** (condition `bedrock:InferenceProfileArn`), in `us-*`. This replaces `bedrock-mantle:CreateInference`, whose resource scoping was not documented. Hermes already used this pattern.
3. **The fallback stays Opus 5.5.** It is not available to the account, so a refusal now ends without a diagnosis, as it would with no fallback. It starts working once the account has access, without a code change.

## **Consequences**
- **Requests can be served in other US regions** (the cross-region profile routes them). Hermes already worked this way; the data stays in the US.
- **Switching back to Mantle** is the client class, the model IDs and the IAM statement, if Mantle serves the model later.
- **The AWS side changed without a run.** The IRSA and AgentCore Runtime policies in `terraform/envs/dev` follow the kind role, which was applied and used; the next AWS run checks them.

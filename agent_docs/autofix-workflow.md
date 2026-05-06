# Autofix Workflow

Automated workflow for discovering and processing PROJQUAY JIRA issues labeled `autofix`.

## Prerequisites

- MCP Atlassian integration configured (for JIRA search and updates)
- ACP session access (for creating agent sessions)

## Workflow Steps

### 1. Discover eligible issues

Use the `acli` tool to find all PROJQUAY issues with the `autofix` label that do **not** have the `autofix-started` label:

- **jql**: `project = PROJQUAY AND labels = "autofix" AND labels != "autofix-started" ORDER BY updated DESC`
- **fields**: `summary,status,issuetype,priority,assignee,labels,updated`
- **limit**: `50`

If zero issues are returned, the workflow ends.

### 2. For each issue, perform the following

#### 2a. Create a new ACP session

Create a session using the `acp_create_session` MCP tool with:

- **session_name**: `autofix-<issue-key>` (lowercased, e.g. `autofix-projquay-12345`)
- **display_name**: `Autofix <ISSUE-KEY>: <summary>`
- **initial_prompt**: The issue key and instructions to begin work (e.g. `/start <ISSUE-KEY>`)
- **repos**: `[{"url": "https://github.com/quay/quay", "branch": "master"}]`
- **workflow_git_url**: `https://github.com/quay/quay`
- **workflow_path**: `agent_docs/workflow.md`

Record the returned session ID.

#### 2b. Comment on the JIRA issue

Use the `/jira` skill to leave a comment on the issue with the session ID:

> Autofix session started: `<session-id>`

#### 2c. Add the `autofix-started` label

Use the `/jira` skill to add the `autofix-started` label to the issue so it is excluded from future runs.

## Flow Diagram

```
mcp__mcp-atlassian__jira_search (JQL query)
         |
         v
   [issues found?] -- no --> done
         |
        yes
         |
         v
   for each issue:
     1. Create ACP session (quay/quay repo + workflow.md)
     2. Comment session ID on JIRA issue (via /jira skill)
     3. Add "autofix-started" label (via /jira skill)
         |
         v
       done
```
# GitHub Setup Guide

This guide explains how to push your local repository to GitHub.

## Prerequisites

1. [Create a GitHub account](https://github.com/signup) if you don't have one
2. [Install Git](https://git-scm.com/downloads) if not already installed
3. [Set up SSH keys](https://docs.github.com/en/authentication/connecting-to-github-with-ssh) for GitHub

## Create GitHub Repository

1. Go to [GitHub](https://github.com)
2. Click "New" to create a new repository
3. Name it "postgres-cdc-reconciliation"
4. Leave it empty (don't initialize with README)

## Push Local Repository

1. Add GitHub remote:
```bash
git remote add origin git@github.com:YOUR_USERNAME/postgres-cdc-reconciliation.git
```

2. Create main branch (if not already on main):
```bash
git branch -M main
```

3. Push to GitHub:
```bash
git push -u origin main
```

## Verify Repository

1. Visit `https://github.com/YOUR_USERNAME/postgres-cdc-reconciliation`
2. You should see:
   - docs/getting_started.md
   - docs/replication_troubleshooting.md
   - Other project files

## Additional Setup

1. Add .gitignore file:
```bash
echo "node_modules/" > .gitignore
echo "*.log" >> .gitignore
echo ".env" >> .gitignore
```

2. Add repository description on GitHub:
   - Go to repository settings
   - Add description: "PostgreSQL CDC replication monitoring with Debezium"
   - Add topics: postgresql, cdc, debezium, replication

## Collaboration

1. Protect main branch:
   - Go to Settings > Branches
   - Add branch protection rule for "main"
   - Require pull request reviews
   - Require status checks to pass

2. Add collaborators:
   - Go to Settings > Collaborators
   - Add team members as needed

## Workflow

1. Create feature branches:
```bash
git checkout -b feature/new-feature
```

2. Make changes and commit:
```bash
git add .
git commit -m "Add new feature"
```

3. Push changes:
```bash
git push origin feature/new-feature
```

4. Create pull request on GitHub:
   - Go to Pull Requests
   - Click "New Pull Request"
   - Select your feature branch
   - Add description and request review
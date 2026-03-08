# RD Observability

Library adding features for observability of the RD apps

Based on [ChatGPT](https://chatgpt.com/c/69a1dba3-ae90-8325-a164-37453193599c)

## Notes

### Linter
-   Der Linter (ruff) mag keine ungenutzten Importe.

### Type checker
-   Mypy
-   Make sure mypy is installed in the conda environment.
-   Install the new package in the conda environment like so:
    - pip install -e .[dev,typecheck] => installs the package with optional dependencies which includes mypy
	- pip install -e .  => installs only the package with its core dependencies
	- **IMPORTANT -e**: This is a flag for EDITABLE INSTALLs. This is important for local development, because it only sets a link to the source directory and does not copy the files into site-packages of my conda installation.
-   Run mypy locally: **mypy src/**

### Local Development and package installation
- Examples
  - pip install -e .     => see above
  - pip install -e .[dev,typecheck]  => see above
  - pip install -e ../basic-helpers  => ../ one level up in directory structure
- **IMPORTANT -e**: This is a flag for EDITABLE INSTALLs. This is important for local development, because it only sets a link to the source directory and does not copy the files into site-packages of my conda installation.
- What happens:
  - Python imports your live source directory
  - You edit code
  - Changes are immediately visible
  - No reinstall needed
- Perfect for development.

#### src/<package_name>src.egg-info

- A folder *.egg.info is created by setuptools which run not only on build but also on pip install . -e (e.g. src/geom_helpers.egg-info)
- Is It Safe to Delete?
  - Yes. You can delete it anytime.

#### Check pip status of a custom package

- pip show basic_helpers 
	If it’s editable, you’ll see something like:
	Location: /home/user/dev/basic_helpers
	Editable project location: /home/user/dev/basic_helpers

#### Inspect Installed Packages

- List installed packages
- Show details of individual package

```
pip list   
pip show basic-helpers     
```

#### How To Know If Something Is Stdlib (Like zoneinfo)?

- Method A — Check Python Documentation
- Method B — CLI Check
  - Run
    ```
    python -c "import zoneinfo; print(zoneinfo.__file__)"
    ```
  - If it prints something like ".../lib/python3.10/zoneinfo/__init__.py" it is part of the Python stdlib.
  - If it prints something like: ".../site-packages/" it is installed via pip/conda.
- Method C — pip show
  - If nothing is returned, it is probably Python stdlib.
- Method D — Check builtins
  ```
  python -c "import sys; print('zoneinfo' in sys.stdlib_module_names)"
  ```
  

#### Important Distinction - Conda vs pip

- Conda manages:
  - Python packages
  - C libraries
  - System libraries
- pip manages:
  - Python packages only

### Venv

There are still problems with the venv setup.

-   The only Python version available 3.8.x.
-   GitHub Copilot found mainly conda, not venv or uv.

### Pytest

- Run **pytest** from package root

#### Check test coverage: 
- Run pytest --cov=geom_helpers.osm_reader_helper --cov-report=term-missing
- shows % of "statements" checked
- list all statements (by line number) which are missing in any given module

### CI

- Local CI is done when changes are committed locally (**only if pre-commit install was run before**)
- Remote CI (on GitHub) is done when committed changes are pushed upstream thanks to GitHub Actions
  - see settings in .github/workflows/ci.yml
  - see also settings in pyproject.toml

#### Remote CI: GitHub Actions

-   First make sure that Pytest works correctly locally, otherwise all CI runs on GitHub will fail.

-   File names with tests **must** start with "test_".

-   on every push to GitHub.com

#### GitHub Authentication

-   GitHub Action workflows are considered external from point of view (POV) of repository and might require authentication.
    - Public repositories can be accessed without authentication
    - **Private repositories require authentication** 
-   Create PAT (Personal Access Token) in GitHub > Settings > Developer Settings
    - Use fine-grained token to limit blast radius => required for CI - specific repos with read-only access for "Content" plus access to metadata (required by default)
      - e.g. geom_helpers needed access to Content of dependency basic_helpers
    - Add the token to the repository settings which will run the CI with all the dependencies
      - e.g. add token to repository settings > Secrets and Variables > Repository Secrets with e.g. name MY_GITHUB_TOKEN
    - Add configuration for authentication to ci.yml (or any other GA workflow for that matter)
      - see updated ci.yml in TEMPLATE folder for that purpose

-   GitHub authentication is also needed for access to private repositories from Kaggle or Colab.


#### Pause Remote CI

-   Rename file from ci.yml to e.g. **ci.yml.pause** or **ci.yml.disabled**

-   Another approach - "Send" certain keywords to GitHub (which scans the commit messages for them) to skip workflows:
    ```git
    git commit -m "debug dependency installation [skip ci]"
    git push
    ```

    GitHub recognizes the following strings in commit messages:
    - [skip ci]
    - [ci skip]
    - [no ci]
    - [skip actions]
    - [actions skip]

#### Local CI

Setting it up

```bash
pip install pre-commit

Create .pre-commit-config.yaml (see example in this template repo)

pre-commit install
```

**!!! pre-commit install** has to be installed **per repository**, i.e. write the required configuration in the .git files. **!!!**

pre-commit install must be run with bash in the osmox environment, because only there "pre-commit" is currently installed.

### GitHub

-   Create empty repo on github.com

-   Either clone new repo to desired location

-   **or better push an existing repository from local machine**
    ```bash
    git remote add origin https://github.com/hgmuc/sudoku2.git
    git branch -M main
    git push -u origin main
    ```

This links the local repository with the remote repository, creates a branch "main" and sets upstream default to "main".
More branches can be created and pushed to by specifying branch names.

**There were issue with this approach when I tried to push changes from my laptop to the server for the first time. Git complained that there was 1 commit on the server and 3 locally, but they were from different bases. A Forced-Update Push fixed this.**

```bash
git push origin main
git push origin develop
git push origin feature/new-ui
```

#### Branching

Clean Branching Strategy (Recommended)

- main → stable releases
- feature/* → development work

Workflow:

1. Branch from main
2. Do work
3. Commit
4. Push branch
5. Open Pull Request
6. Merge into main
7. Tag release

Create and swich

-   Create: git checkout -b feature/type-checking
-   Switch: git switch -c feature/type-checking
-   Work
-   Commit
-   Push: git push -u origin feature/type-checking

Switching

-   git switch main

#### Semantic versioning

MAJOR.MINOR.PATCH

| Type  | When to increment                |
| ----- | -------------------------------- |
| MAJOR | Breaking change                  |
| MINOR | New feature, backward compatible |
| PATCH | Bugfix                           |

When you're ready to release:
Tagging

```git
git tag v0.2.0
git push origin v0.2.0
```

Releasing
Bump version in pyproject.toml:
[project]
version = "0.2.0"

Commit

```git
git commit -am "Release 0.2.0"
```

Tagging

```git
git tag v0.2.0
```

Push

```git
git push
git push --tags
```

## Kaggle / Colab

### Options to load private repositories into Kaggle

#### Wheels plus upload as Dataset

- Build wheel from current version: python -m build 

- Build wheel from specific version
	- Check diff to current version: git status
```
git checkout v0.2.0
pip wheel . --no-deps -w ../wheels
git switch -
```

or 

```
git -C basic_helpers checkout v0.2.0
pip wheel ./basic_helpers --no-deps -w wheels/
git -C basic_helpers switch -
```

	- _git switch <tag>_ and _git checkout <tag>_ puts you into a detached HEAD state
	- _git switch -_ or _git checkout -_  returns to HEAD
	- The -C lets you run Git commands without changing directories.

- Upload wheel(s) to a dataset (multiple / all wheels in one dataset)
- A dataset can contain wheels of different versions of the same package => which version is being installed depends on the pip install directives
- !pip install ...

```
!pip install /kaggle/input/datasets/hgassner/wheels-test/basic_helpers-0.2.1-py3-none-any.whl
!pip install /kaggle/input/datasets/hgassner/wheels-test/geom_helpers-0.1.2-py3-none-any.whl
```

#### Kaggle Dependency Manager and install from GitHub

- Create a Kaggle Secret for a GitHub PAT (already done! => GH_TOKEN)
- Kaggle Dependency Manager (see menu Add-ons > Install dependencies)
- Add pip install directives into Dependency Manager box
	- Use placeholder for GH_TOKEN
	- No spaces in package name / URL

In notebook  
```
from kaggle_secrets import UserSecretsClient
user_secrets = UserSecretsClient()
GH_TOKEN = user_secrets.get_secret("GH_TOKEN")
```

In Dependency Manager  
```
!pip install basic_helpers@git+https://x-access-token:${GH_TOKEN}@github.com/hgmuc/basic_helpers.git@v0.2.1
!pip install geom_helpers@git+https://x-access-token:${GH_TOKEN}@github.com/hgmuc/geom_helpers.git@v0.1.2
```
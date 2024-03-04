# Instructions
Make sure you have `python3.9` installed

## Setup your virtual environment named **dev**
`python3.9 -m venv dev`

## Activate virtual environment
`source dev/bin/activate`

In case you run into permission issues try the following.

`chmod +x .`

## Download required dependencies
`pip install -r requirements.txt`

## Run program
Place your key in the project root `stocks-poc-key.json`
`python main.py`

When done terminate the program and run `deactivate` to terminate virtual environment.
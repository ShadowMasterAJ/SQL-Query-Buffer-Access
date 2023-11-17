# CZ4031 PROJECT 2: WHAT IS YOUR QUERY ACCESSING & AT WHAT COST?

## Installation
Highly recommended that you install within a virtual environment.

### Prerequisites
- Node.js: Make sure you have Node.js installed. You can download it from [nodejs.org](https://nodejs.org/).
- PostgreSQL is set up with the necessary tables (TPC-H) as stated in the project requirements, and have the credentials ready.

### Setting Up
1. Install necessary Python libraries as stated in requirements.txt:
```pip install -r requirements.txt```

2. Navigate to the pev2_component directory and install npm dependencies
```cd pev2_component```
```npm install```

### Run the project
```python project.py```

## Usage
Users should interact with the project through the graphical user interface (GUI). 
1. Login to the database using your account.
2. Enter the query, whose QEP you want to extract, in the query window.
3. Click on the execute query button to retrieve the QEP.
4. On the right side you will find the blocks in each relation and their ids.
5. Click on the block id to get the contents stored inside the block. 
    5.1 The records shown for block ids shown in tuples are the contents for the blocks with ids ranging from value 1 to value 2 of the tuple. 
    5.2 Eg. Clicking on block id: (1,10) would show the records stored in blocks 1 to 10.
6. Click on visualise QEP to get a graphical representation of the QEP. Note that this button will launch a localhost server, containing the graph, in your default browser.

## Troubleshooting
- If you encounter any issues related to missing modules or dependencies, make sure to check the installation steps and update your package.json accordingly.
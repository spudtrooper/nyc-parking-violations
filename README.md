# nyc-parking-violations

Finds the total a given license plate owes in parking tickets.

## Usage

Install

```bash
go install github.com/spudtrooper/nyc-parking-violations
```

then, e.g. if license plate `ABCDEFG` owed $1234.56:

```bash
~/go/bin/nyc-parking-violations --plate ABCDEFG

$1234.56
```
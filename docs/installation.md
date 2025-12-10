# Installation

## Prerequisites

- Python 3.8+
- Rust (for building the library)
- PostgreSQL or MySQL server
- `maturin` for Rust-Python integration

## Install sqlrustler

1. Clone the repository:
   ```bash
   git clone https://github.com/DVNghiem/SqlRustler.git
   cd SqlRustler
   ```
2. Install Python dependencies (if any):
   ```bash
   pip3 install poetry maturin[patchelf]
   poetry install
   ```

3. Build and install the library using `maturin`:
   ```bash
   maturin develop
   ```
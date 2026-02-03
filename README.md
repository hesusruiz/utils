# utils
Assorted utils for Go programs which I reuse across projects

## Contributing

Contributions are welcome! Whether you're fixing a bug, improving an existing utility, or adding a new one, this guide will help you get started.

### Development Setup

This repository is structured as a Go workspace, containing multiple independent modules (utilities). The `go.work` file in the root directory manages this setup.

The `go.work` file makes it easy to work on multiple modules simultaneously. When you import one utility into another (e.g., for testing), Go will use the local code in this repository instead of trying to download it from its remote source. This is the standard and recommended way to work with multi-module projects in Go.

To set up your environment:
1. Fork the repository.
2. Clone your fork to your local machine.
3. The `go.work` file is already configured. You can start coding.

### Adding a New Utility

To add a new utility:
1. Create a new directory for your utility in the root of the repository (e.g., `mynewutil`).
2. Create your Go source files inside the new directory (e.g., `mynewutil/mynewutil.go`).
3. Initialize it as a Go module:
   ```sh
   cd mynewutil
   go mod init github.com/hesusruiz/utils/mynewutil
   cd ..
   ```
4. Add the new module to the `go.work` file. You can do this by running the following command from the root directory:
   ```sh
   go work use ./mynewutil
   ```

### Submitting Changes

1. **Create a new branch:**
   ```sh
   git checkout -b feature/my-awesome-feature
   ```
2. **Make your changes.**
3. **Commit your changes** with a clear and descriptive message.
4. **Push your branch** to your fork:
   ```sh
   git push origin feature/my-awesome-feature
   ```
5. **Create a Pull Request** from your fork's branch to the `main` branch of the original repository.
from pathlib import Path
from typing import Dict, Optional

from src.commit_service import (
    create_version_from_head,
    create_version_from_raw_default,
)
from src.diff_service import compare_versions
from src.errors import DataLineageError
from src.repo import RepoState


def _print_header() -> None:
    print("\n" + "=" * 72)
    print("DataLineage Prototype (CPU-only | Filesystem-only)")
    print("Track text dataset versions with deterministic preprocessing + hashing")
    print("=" * 72)


def _print_menu() -> None:
    print("\nChoose an action:")
    print("  1) Initialize / Show Repository Status")
    print("     - Confirms .mydata structure, HEAD pointer, and logs file")
    print("  2) Commit from Current HEAD + Config")
    print("     - Uses current HEAD version as input, applies provided config")
    print("  3) Input of Raw Dataset (Default Preprocessing)")
    print("     - Takes raw dataset path + message, applies built-in deterministic preprocessing and saves the new version")
    print("  4) List Versions")
    print("     - Shows committed versions with parent linkage and summary")
    print("  5) Checkout Version")
    print("     - Shows versions, then lets you select directly or view one first")
    print("  6) View Specific Version Logs")
    print("     - Shows detailed metadata for only the version you request")
    print("  7) Compare Two Versions")
    print("     - Shows concise differences and stores detailed diff report")
    print("  8) Exit")


def _show_init_status(repo: RepoState) -> Dict[str, str]:
    head = repo.get_head()
    log_count = len(repo.read_logs())
    print("\nRepository status:")
    print(f"Project root: {repo.project_root}")
    print(f"Internal repo: {repo.repo_root}")
    print(f"Versions path: {repo.versions_root}")
    print(f"Current HEAD: {head}")
    print(f"Total logged versions: {log_count}")
    return {
        "Command": "init/status",
        "Project Root": str(repo.project_root),
        "Current HEAD": str(head),
        "Log Entries": str(log_count),
    }


def _commit_from_head_flow(repo: RepoState) -> Dict[str, str]:
    print("\nCommit from Current HEAD + Config")
    current_head = repo.get_head()
    print(f"Current HEAD: {current_head}")
    if not current_head:
        raise DataLineageError(
            "HEAD is not set. First create a version using raw dataset input."
        )

    current_record = _find_version_record(repo, current_head)
    if current_record:
        print("Current HEAD version summary:")
        print(f"  version_id: {current_record.get('version_id')}")
        print(f"  parent_id: {current_record.get('parent_id')}")
        print(f"  timestamp: {current_record.get('timestamp')}")
        print(f"  rows: {current_record.get('row_count')}")
        print(f"  message: {current_record.get('commit_message')}")

    config_path = input("Config file path (.json): ").strip()
    commit_message = input("Commit message: ").strip()

    if not config_path:
        raise DataLineageError("Config path is required for HEAD commit mode.")
    if not commit_message:
        raise DataLineageError("Commit message is required.")

    result = create_version_from_head(
        repo=repo,
        config_path=config_path,
        commit_message=commit_message,
    )

    if result["status"] == "duplicate":
        print("\nNo new version created.")
        print(f"Reason: {result['message']}")
        print(f"Existing version: {result['version_id']}")
        return {
            "Command": "commit-head",
            "Status": "duplicate",
            "Mode": "head-config",
            "Dataset Path": "HEAD",
            "Config Path": config_path,
            "Resolved Version": result["version_id"],
            "HEAD": str(result["head"]),
        }

    print("\nVersion created successfully.")
    print(f"Version ID: {result['version_id']}")
    print(f"Parent ID: {result['parent_id']}")
    print(f"Rows before -> after: {result['rows_before']} -> {result['rows_after']}")
    print(f"Saved at: {result['version_path']}")
    return {
        "Command": "commit-head",
        "Status": "created",
        "Mode": "head-config",
        "Dataset Path": "HEAD",
        "Config Path": config_path,
        "Version ID": result["version_id"],
        "Parent ID": str(result["parent_id"]),
        "HEAD": str(result["head"]),
    }


def _commit_from_raw_flow(repo: RepoState) -> Dict[str, str]:
    print("\nCommit from Raw Dataset (Default Preprocessing)")
    dataset_path = input("Raw dataset file path (CSV/JSON): ").strip()
    commit_message = input("Commit message: ").strip()

    if not dataset_path:
        raise DataLineageError("Dataset path is required for raw commit mode.")
    if not commit_message:
        raise DataLineageError("Commit message is required.")

    result = create_version_from_raw_default(
        repo=repo,
        dataset_path=dataset_path,
        commit_message=commit_message,
    )

    if result["status"] == "duplicate":
        print("\nNo new version created.")
        print(f"Reason: {result['message']}")
        print(f"Existing version: {result['version_id']}")
        return {
            "Command": "commit-raw",
            "Status": "duplicate",
            "Mode": "raw-default",
            "Dataset Path": dataset_path,
            "Config Path": "DEFAULT_CONFIG",
            "Resolved Version": result["version_id"],
            "HEAD": str(result["head"]),
        }

    print("\nVersion created successfully.")
    print(f"Version ID: {result['version_id']}")
    print(f"Parent ID: {result['parent_id']}")
    print(f"Rows before -> after: {result['rows_before']} -> {result['rows_after']}")
    print(f"Saved at: {result['version_path']}")
    return {
        "Command": "commit-raw",
        "Status": "created",
        "Mode": "raw-default",
        "Dataset Path": dataset_path,
        "Config Path": "DEFAULT_CONFIG",
        "Version ID": result["version_id"],
        "Parent ID": str(result["parent_id"]),
        "HEAD": str(result["head"]),
    }


def _list_versions(repo: RepoState) -> Dict[str, str]:
    logs = [
        record
        for record in repo.read_logs()
        if record.get("event_type", "commit") == "commit" and record.get("version_id")
    ]
    head = repo.get_head()
    print(f"\nCurrent HEAD: {head}")

    if not logs:
        print("\nNo committed versions yet.")
        return {
            "Command": "list-versions",
            "Total Versions": "0",
        }

    print("\nCommitted versions:")
    current_head = head
    for index, record in enumerate(logs, start=1):
        version_id = str(record.get("version_id"))
        parent_id = str(record.get("parent_id"))
        message = str(record.get("commit_message", ""))
        rows = str(record.get("row_count", "-"))
        marker = " <- HEAD" if current_head == version_id else ""
        print(
            f"{index}. version={version_id} | parent={parent_id} | rows={rows}{marker}"
        )
        print(f"   message: {message}")

    return {
        "Command": "list-versions",
        "Total Versions": str(len(logs)),
        "Current HEAD": str(head),
    }


def _find_version_record(repo: RepoState, version_id: str) -> Optional[Dict]:
    for record in repo.read_logs():
        if record.get("event_type", "commit") != "commit":
            continue
        if str(record.get("version_id")) == version_id:
            return record
    return None


def _print_version_details(record: Dict) -> None:
    print("\nVersion details:")
    print(f"version_id: {record.get('version_id')}")
    print(f"parent_id: {record.get('parent_id')}")
    print(f"timestamp: {record.get('timestamp')}")
    print(f"commit_message: {record.get('commit_message')}")
    print(f"source_data_path: {record.get('source_data_path')}")
    print(f"source_config_path: {record.get('source_config_path')}")
    print(f"input_hash: {record.get('input_hash')}")
    print(f"config_hash: {record.get('config_hash')}")
    print(f"version_hash: {record.get('version_hash')}")
    print(f"row_count: {record.get('row_count')}")


def _select_version_interactively(repo: RepoState) -> Optional[str]:
    logs = [
        record
        for record in repo.read_logs()
        if record.get("event_type", "commit") == "commit" and record.get("version_id")
    ]
    if not logs:
        raise DataLineageError("No versions available to checkout.")

    while True:
        current_head = repo.get_head()
        print("\nCurrent menu: Checkout Version")
        print(f"Current HEAD: {current_head}")
        print("Available versions:")
        for index, record in enumerate(logs, start=1):
            version_id = str(record.get("version_id"))
            message = str(record.get("commit_message", ""))
            marker = " <- HEAD" if current_head == version_id else ""
            print(f"{index}. {version_id}{marker}")
            print(f"   message: {message}")

        print("\nCheckout options:")
        print("  1) Direct select for checkout")
        print("  2) View specific version details first")
        print("  3) Exit from Current Menu")
        option = input("Choose option (1/2/3): ").strip()

        if option == "3":
            return None

        if option == "2":
            version_to_view = input("Enter version ID to view details: ").strip()
            if not version_to_view:
                print("Version ID is required.")
                continue
            record = _find_version_record(repo, version_to_view)
            if record is None:
                print(f"Version not found: {version_to_view}")
                continue
            _print_version_details(record)
            proceed = input("Checkout this version now? (y/n): ").strip().lower()
            if proceed == "y":
                return version_to_view
            if proceed == "n":
                continue
            print("Invalid selection. Returning to checkout options.")
            continue

        if option == "1":
            selection = input(
                "Enter version index or exact version ID for checkout: "
            ).strip()
            if not selection:
                print("Version selection is required.")
                continue

            if selection.isdigit():
                selected_index = int(selection)
                if selected_index < 1 or selected_index > len(logs):
                    print("Invalid version index.")
                    continue
                return str(logs[selected_index - 1].get("version_id"))

            return selection

        print("Invalid option. Please choose 1, 2, or 3.")


def _checkout_flow(repo: RepoState) -> Dict[str, str]:
    target_version = _select_version_interactively(repo)
    if target_version is None:
        print("\nCheckout cancelled. Exiting current menu.")
        return {
            "Command": "checkout",
            "Status": "cancelled",
        }

    if not repo.version_exists(target_version):
        raise DataLineageError(f"Version not found: {target_version}")

    previous_head = repo.get_head()
    repo.set_head(target_version)

    print("\nCheckout successful.")
    print(f"Previous HEAD: {previous_head}")
    print(f"Current HEAD: {target_version}")
    return {
        "Command": "checkout",
        "Status": "success",
        "Previous HEAD": str(previous_head),
        "Current HEAD": target_version,
    }


def _view_specific_version_logs(repo: RepoState) -> Dict[str, str]:
    target_version = input("\nEnter version ID to view logs: ").strip()
    if not target_version:
        raise DataLineageError("Version ID is required.")

    record = _find_version_record(repo, target_version)
    if record is None:
        raise DataLineageError(f"Version not found in logs: {target_version}")

    _print_version_details(record)
    return {
        "Command": "view-version-logs",
        "Status": "shown",
        "Version ID": target_version,
    }


def _compare_versions_flow(repo: RepoState) -> Dict[str, str]:
    version_a = input("\nEnter base version ID (A): ").strip()
    version_b = input("Enter target version ID (B): ").strip()

    if not version_a or not version_b:
        raise DataLineageError("Both version IDs are required for compare.")

    result = compare_versions(repo=repo, version_a=version_a, version_b=version_b)

    print("\nDiff summary:")
    print(f"A rows: {result['row_count_a']}")
    print(f"B rows: {result['row_count_b']}")
    print(f"Row delta (B-A): {result['row_delta']}")
    print(f"Config changed: {result['config_changed']}")
    print(f"Label distribution changed: {result['label_distribution_changed']}")
    print(f"Detailed report saved: {result['report_path']}")

    return {
        "Command": "compare-versions",
        "Status": "done",
        "Version A": version_a,
        "Version B": version_b,
        "Report": result["report_path"],
    }


def main() -> None:
    repo = RepoState(project_root=Path(__file__).resolve().parent)

    while True:
        _print_header()
        _print_menu()

        choice = input("\nEnter option number: ").strip()

        try:
            if choice == "1":
                _show_init_status(repo)
            elif choice == "2":
                _commit_from_head_flow(repo)
            elif choice == "3":
                _commit_from_raw_flow(repo)
            elif choice == "4":
                _list_versions(repo)
            elif choice == "5":
                _checkout_flow(repo)
            elif choice == "6":
                _view_specific_version_logs(repo)
            elif choice == "7":
                _compare_versions_flow(repo)
            elif choice == "8":
                print("\nExiting DataLineage Prototype. Goodbye.")
                break
            else:
                print("\nInvalid option. Please choose 1, 2, 3, 4, 5, 6, 7, or 8.")
        except DataLineageError as error:
            print(f"\nOperation failed: {error}")
        except Exception as error:
            print(f"\nUnexpected error: {error}")


if __name__ == "__main__":
    main()

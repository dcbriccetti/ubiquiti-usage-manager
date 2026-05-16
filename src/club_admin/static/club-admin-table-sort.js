(() => {
  const collator = new Intl.Collator(undefined, {
    numeric: true,
    sensitivity: "base",
  });

  const cellSortValue = (row, columnIndex) => {
    const cell = row.cells[columnIndex];
    return (cell?.dataset.sortValue ?? cell?.textContent ?? "").trim();
  };

  const comparableValue = (value, sortType) => {
    if (!value) {
      return null;
    }
    if (sortType === "number") {
      const numberValue = Number.parseFloat(value.replace(/,/g, ""));
      return Number.isNaN(numberValue) ? null : numberValue;
    }
    if (sortType === "date") {
      const dateValue = Date.parse(value);
      return Number.isNaN(dateValue) ? null : dateValue;
    }
    return value;
  };

  const compareRows = (left, right, columnIndex, sortType, direction) => {
    const leftValue = comparableValue(cellSortValue(left.row, columnIndex), sortType);
    const rightValue = comparableValue(cellSortValue(right.row, columnIndex), sortType);
    const leftIsBlank = leftValue === null;
    const rightIsBlank = rightValue === null;

    if (leftIsBlank || rightIsBlank) {
      if (leftIsBlank && rightIsBlank) {
        return left.index - right.index;
      }
      return leftIsBlank ? 1 : -1;
    }

    const comparison =
      typeof leftValue === "number" && typeof rightValue === "number"
        ? leftValue - rightValue
        : collator.compare(String(leftValue), String(rightValue));

    if (comparison === 0) {
      return left.index - right.index;
    }
    return direction === "asc" ? comparison : -comparison;
  };

  const updateSortState = (table, activeButton, direction) => {
    table.querySelectorAll("thead th").forEach((header) => {
      header.setAttribute("aria-sort", "none");
    });
    table.querySelectorAll(".sortable-heading").forEach((button) => {
      button.dataset.sortDirection = "";
    });

    activeButton.dataset.sortDirection = direction;
    activeButton.closest("th")?.setAttribute(
      "aria-sort",
      direction === "asc" ? "ascending" : "descending",
    );
  };

  const sortTable = (table, button) => {
    const tbody = table.tBodies[0];
    if (!tbody) {
      return;
    }

    const columnIndex = Number.parseInt(button.dataset.sortColumn ?? "", 10);
    if (Number.isNaN(columnIndex)) {
      return;
    }

    const rows = Array.from(tbody.rows);
    if (rows.length < 2) {
      return;
    }

    const currentDirection = button.dataset.sortDirection;
    const nextDirection = currentDirection === "asc" ? "desc" : "asc";
    const sortType = button.dataset.sortType || "text";
    const sortedRows = rows
      .map((row, index) => ({ row, index }))
      .sort((left, right) =>
        compareRows(left, right, columnIndex, sortType, nextDirection),
      );

    tbody.append(...sortedRows.map(({ row }) => row));
    updateSortState(table, button, nextDirection);
  };

  const initializeSortableTables = () => {
    document.querySelectorAll("[data-sortable-table]").forEach((table) => {
      table.querySelectorAll(".sortable-heading").forEach((button) => {
        button.addEventListener("click", () => sortTable(table, button));
      });
    });
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initializeSortableTables);
  } else {
    initializeSortableTables();
  }
})();

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

  const sortableRows = (table) =>
    Array.from(
      table.tBodies[0]?.querySelectorAll(
        "tr[data-sortable-row], tr[data-user-row]",
      ) ?? [],
    );

  const storedSortKey = (table) => table.dataset.persistSortKey || "";

  const readStoredSort = (table) => {
    const key = storedSortKey(table);
    if (!key) {
      return null;
    }
    try {
      const value = JSON.parse(window.localStorage.getItem(key) || "null");
      if (
        value &&
        Number.isInteger(value.column) &&
        (value.direction === "asc" || value.direction === "desc")
      ) {
        return value;
      }
    } catch {
      window.localStorage.removeItem(key);
    }
    return null;
  };

  const writeStoredSort = (table, columnIndex, direction) => {
    const key = storedSortKey(table);
    if (!key) {
      return;
    }
    try {
      window.localStorage.setItem(
        key,
        JSON.stringify({ column: columnIndex, direction }),
      );
    } catch {
      // Sorting still works if browser storage is unavailable.
    }
  };

  const searchableRows = (table) =>
    Array.from(table.tBodies[0]?.querySelectorAll("tr[data-user-row]") ?? []);

  const searchableCellValue = (row, columnIndex) => {
    const cell = row.cells[columnIndex];
    return (cell?.dataset.sortValue ?? cell?.textContent ?? "").trim().toLowerCase();
  };

  const rowMatchesQuery = (row, query, columnValue) => {
    if (!query) {
      return true;
    }
    if (columnValue === "all") {
      return Array.from(row.cells).some((_, columnIndex) =>
        searchableCellValue(row, columnIndex).includes(query),
      );
    }

    const columnIndex = Number.parseInt(columnValue, 10);
    return (
      !Number.isNaN(columnIndex) &&
      searchableCellValue(row, columnIndex).includes(query)
    );
  };

  const updateSearchCount = (countElement, visibleCount, totalCount) => {
    if (!countElement) {
      return;
    }
    if (totalCount === 0) {
      countElement.value = "";
      countElement.textContent = "";
      return;
    }
    const userLabel = visibleCount === 1 ? "user" : "users";
    const countText =
      visibleCount === totalCount
        ? `(${visibleCount.toLocaleString()})`
        : `(${visibleCount.toLocaleString()} of ${totalCount.toLocaleString()} ${userLabel})`;
    countElement.value = countText;
    countElement.textContent =
      countText;
  };

  const applySearch = (table, searchInput, columnSelect, countElement) => {
    const query = searchInput.value.trim().toLowerCase();
    const columnValue = columnSelect?.value || "all";
    const rows = searchableRows(table);
    let visibleCount = 0;

    rows.forEach((row) => {
      const isVisible = rowMatchesQuery(row, query, columnValue);
      row.hidden = !isVisible;
      if (isVisible) {
        visibleCount += 1;
      }
    });

    const placeholder = table.tBodies[0]?.querySelector("[data-search-placeholder]");
    if (placeholder) {
      placeholder.hidden = !query || visibleCount > 0 || rows.length === 0;
    }
    updateSearchCount(countElement, visibleCount, rows.length);
  };

  const updateSearchResetState = (searchInput, columnSelect, resetButton) => {
    if (!resetButton) {
      return;
    }
    resetButton.disabled =
      !searchInput.value.trim() && (!columnSelect || columnSelect.value === "all");
  };

  const checkinCheckboxes = (container) =>
    Array.from(container.querySelectorAll("[data-checkin-checkbox]"));

  const checkedCheckinCount = (container) =>
    checkinCheckboxes(container).filter((checkbox) => checkbox.checked).length;

  const updateBulkCheckinState = (container) => {
    const submitButton = container.querySelector("[data-checkin-submit]");
    const checkedCount = checkedCheckinCount(container);

    if (submitButton) {
      submitButton.disabled = checkedCount === 0;
      if (checkedCount === 0) {
        submitButton.textContent = "Check In Selected";
      } else {
        submitButton.textContent =
          checkedCount === 1 ? "Check In 1 Selected" : `Check In ${checkedCount} Selected`;
      }
    }
  };

  const confirmBulkCheckin = (container) => {
    const checkedCount = checkedCheckinCount(container);
    if (checkedCount <= 2) {
      return true;
    }
    return window.confirm(`Check in ${checkedCount} selected users?`);
  };

  const resetSearch = (table, searchInput, columnSelect, countElement, resetButton) => {
    searchInput.value = "";
    if (columnSelect) {
      columnSelect.value = "all";
    }
    searchableRows(table).forEach((row) => {
      row.hidden = false;
    });
    const placeholder = table.tBodies[0]?.querySelector("[data-search-placeholder]");
    if (placeholder) {
      placeholder.hidden = true;
    }
    updateSearchCount(countElement, searchableRows(table).length, searchableRows(table).length);
    updateSearchResetState(searchInput, columnSelect, resetButton);
    searchInput.focus();
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

  const sortTable = (table, button, options = {}) => {
    const tbody = table.tBodies[0];
    if (!tbody) {
      return;
    }

    const columnIndex = Number.parseInt(button.dataset.sortColumn ?? "", 10);
    if (Number.isNaN(columnIndex)) {
      return;
    }

    const rows = sortableRows(table);
    if (rows.length < 2) {
      return;
    }

    const currentDirection = button.dataset.sortDirection;
    const nextDirection =
      options.direction || (currentDirection === "asc" ? "desc" : "asc");
    const sortType = button.dataset.sortType || "text";
    const sortedRows = rows
      .map((row, index) => ({ row, index }))
      .sort((left, right) =>
        compareRows(left, right, columnIndex, sortType, nextDirection),
      );

    const placeholder = tbody.querySelector("[data-search-placeholder]");
    tbody.append(...sortedRows.map(({ row }) => row));
    if (placeholder) {
      tbody.append(placeholder);
    }
    updateSortState(table, button, nextDirection);
    if (options.persist !== false) {
      writeStoredSort(table, columnIndex, nextDirection);
    }
  };

  const initializeSortableTables = () => {
    document.querySelectorAll("[data-sortable-table]").forEach((table) => {
      const headings = Array.from(table.querySelectorAll(".sortable-heading"));
      headings.forEach((heading) => {
        heading.addEventListener("click", () => sortTable(table, heading));
        if (heading.tagName !== "BUTTON") {
          heading.addEventListener("keydown", (event) => {
            if (event.key !== "Enter" && event.key !== " ") {
              return;
            }
            event.preventDefault();
            sortTable(table, heading);
          });
        }
      });
      const storedSort = readStoredSort(table);
      if (storedSort) {
        const heading = headings.find(
          (candidate) =>
            Number.parseInt(candidate.dataset.sortColumn ?? "", 10) ===
            storedSort.column,
        );
        if (heading) {
          sortTable(table, heading, {
            direction: storedSort.direction,
            persist: false,
          });
        }
      }

      const container = table.closest(".users-page") ?? document;
      const searchInput = container.querySelector("[data-table-search]");
      const columnSelect = container.querySelector("[data-table-search-column]");
      const countElement = container.querySelector("[data-table-search-count]");
      const resetButton = container.querySelector("[data-table-search-reset]");
      if (searchInput) {
        const updateSearch = () => {
          applySearch(table, searchInput, columnSelect, countElement);
          updateSearchResetState(searchInput, columnSelect, resetButton);
          updateBulkCheckinState(container);
        };

        searchInput.addEventListener("input", updateSearch);
        searchInput.addEventListener("search", updateSearch);
        columnSelect?.addEventListener("change", updateSearch);
        resetButton?.addEventListener("click", () =>
          resetSearch(table, searchInput, columnSelect, countElement, resetButton),
        );
        applySearch(table, searchInput, columnSelect, countElement);
        updateSearchResetState(searchInput, columnSelect, resetButton);
      }

      checkinCheckboxes(container).forEach((checkbox) => {
        checkbox.addEventListener("change", () => updateBulkCheckinState(container));
      });
      container.querySelector("#members-checkin-form")?.addEventListener("submit", (event) => {
        if (!confirmBulkCheckin(container)) {
          event.preventDefault();
        }
      });
      updateBulkCheckinState(container);
    });
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initializeSortableTables);
  } else {
    initializeSortableTables();
  }
})();

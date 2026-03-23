import 'dart:async';

import 'package:drift/drift.dart';
import 'package:drift_sqlite_async/src/executor.dart';
import 'package:sqlite_async/sqlite_async.dart';

/// Wraps a sqlite_async [SqliteConnection] as a Drift [DatabaseConnection].
///
/// The SqliteConnection must be instantiated before constructing this, and
/// is not closed when [SqliteAsyncDriftConnection.close] is called.
///
/// This class handles delegating Drift's queries and transactions to the
/// [SqliteConnection], and passes on any table updates from the
/// [SqliteConnection] to Drift.
class SqliteAsyncDriftConnection extends DatabaseConnection {
  late StreamSubscription _updateSubscription;
  bool _updatesDropped = false;

  /// Pending table updates accumulated during the debounce window.
  final Set<TableUpdate> _debouncePending = {};

  /// Table updates buffered while [_updatesDropped] is true.
  ///
  /// Fired as a single consolidated notification on [resumeUpdates].
  final Set<TableUpdate> _droppedPending = {};

  /// Timer for debouncing update notifications.
  Timer? _debounceTimer;

  /// Debounce duration for accumulating table update notifications.
  ///
  /// Multiple rapid updates (e.g. from sync) are consolidated into a single
  /// notification, reducing the number of watch-query re-fires.
  static const _debounceDuration = Duration(milliseconds: 100);

  /// Suppresses table-update forwarding from PowerSync to Drift.
  ///
  /// While suppressed, updates are buffered (not dropped). Call
  /// [resumeUpdates] to fire a single consolidated notification so that all
  /// watches refresh once with the latest data.
  void pauseUpdates() {
    _updatesDropped = true;
    // Flush any pending debounced updates before pausing so they aren't lost.
    _debounceTimer?.cancel();
    _flushPendingUpdates();
  }

  /// Resumes table-update forwarding previously suppressed by [pauseUpdates].
  ///
  /// Fires a single consolidated notification for all updates that arrived
  /// during the pause, so every watch query refreshes once.
  void resumeUpdates() {
    _updatesDropped = false;
    if (_droppedPending.isNotEmpty) {
      final updates = Set<TableUpdate>.of(_droppedPending);
      _droppedPending.clear();
      super.streamQueries.handleTableUpdates(updates);
    }
  }

  /// [transformTableUpdates] is useful to map local table names from PowerSync that are backed by a view name
  /// which is the entity that the user interacts with.
  SqliteAsyncDriftConnection(
    SqliteConnection db, {
    bool logStatements = false,
    Set<TableUpdate> Function(UpdateNotification)? transformTableUpdates,
  }) : super(SqliteAsyncQueryExecutor(db, logStatements: logStatements)) {
    _updateSubscription = (db as SqliteQueries).updates!.listen((event) {
      final Set<TableUpdate> setUpdates;
      if (transformTableUpdates != null) {
        setUpdates = transformTableUpdates(event);
      } else {
        setUpdates = <TableUpdate>{};
        for (var tableName in event.tables) {
          setUpdates.add(TableUpdate(tableName));
        }
      }

      if (_updatesDropped) {
        _droppedPending.addAll(setUpdates);
        return;
      }

      // Accumulate updates and debounce: instead of firing every notification
      // immediately, we collect table names and flush once per 100ms window.
      // This reduces ~70 rapid sync notifications to ~10-15 consolidated ones.
      _debouncePending.addAll(setUpdates);
      _debounceTimer?.cancel();
      _debounceTimer = Timer(_debounceDuration, _flushPendingUpdates);
    });
  }

  void _flushPendingUpdates() {
    if (_debouncePending.isEmpty) return;
    final updates = Set<TableUpdate>.of(_debouncePending);
    _debouncePending.clear();
    super.streamQueries.handleTableUpdates(updates);
  }

  @override
  Future<void> close() async {
    _debounceTimer?.cancel();
    _flushPendingUpdates();
    await _updateSubscription.cancel();
    await super.close();
  }
}

defmodule Campfire.DBStatementCacheTest do
  use ExUnit.Case, async: true
  alias Campfire.DB
  alias Exqlite.Sqlite3, as: SQL

  @moduletag :tmp_dir

  setup %{tmp_dir: dir} do
    path = Path.join(dir, "statements.sqlite3")
    {:ok, writer} = SQL.open(path)

    :ok =
      SQL.execute(writer, """
      PRAGMA journal_mode=WAL;
      CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);
      INSERT INTO items VALUES (1, 'one'), (2, 'two');
      """)

    {:ok, {reader, statements} = conn, _} = DB.Connection.init_worker({:reader, path})

    on_exit(fn ->
      SQL.close(reader)
      SQL.close(writer)
      File.rm_rf!(dir)
    end)

    %{writer: writer, reader: reader, conn: conn, statements: statements, path: path}
  end

  test "SELECT * uses current columns after an external schema change", %{
    writer: writer,
    conn: conn
  } do
    sql = "SELECT * FROM items WHERE id=?"
    assert [%{"id" => 1, "name" => "one"}] = DB.run(conn, sql, [1])

    :ok = SQL.execute(writer, "ALTER TABLE items ADD COLUMN color TEXT DEFAULT 'red'")
    assert [%{"id" => 2, "name" => "two", "color" => "red"}] = DB.run(conn, sql, [2])

    :ok = SQL.execute(writer, "ALTER TABLE items RENAME COLUMN name TO label")
    assert [%{"id" => 1, "label" => "one", "color" => "red"}] = DB.run(conn, sql, [1])
  end

  test "a partial bind failure evicts and finalizes only the failed statement", %{
    conn: conn,
    reader: reader,
    statements: statements
  } do
    sql = "SELECT ? AS a, ? AS b"
    assert [%{"a" => 1, "b" => "first"}] = DB.run(conn, sql, [1, "first"])
    [{^sql, failed}] = :ets.lookup(statements, sql)
    assert [%{"name" => "one"}] = DB.run(conn, "SELECT name FROM items WHERE id=?", [1])
    [unrelated] = :ets.lookup(statements, "SELECT name FROM items WHERE id=?")

    assert_raise ArgumentError, fn -> DB.run(conn, sql, ["partial", {:unsupported}]) end

    assert [] == :ets.lookup(statements, sql)
    assert {:error, _} = SQL.multi_step(reader, failed)
    assert [unrelated] == :ets.lookup(statements, "SELECT name FROM items WHERE id=?")
    assert [%{"a" => nil, "b" => 3}] = DB.run(conn, sql, [nil, 3])
    assert [%{"a" => 4.5, "b" => nil}] = DB.run(conn, sql, [4.5, nil])
  end

  test "a step error releases the WAL snapshot and the failed statement", %{
    conn: conn,
    writer: writer,
    statements: statements
  } do
    sql = "SELECT CASE WHEN id=2 THEN json('{bad') ELSE id END AS value FROM items ORDER BY id"
    assert_raise MatchError, fn -> DB.run(conn, sql, []) end
    assert [] == :ets.lookup(statements, sql)

    :ok = SQL.execute(writer, "UPDATE items SET name='changed' WHERE id=1")
    {:ok, checkpoint} = SQL.prepare(writer, "PRAGMA wal_checkpoint(PASSIVE)")

    try do
      assert {:ok, [[0, frames, frames]]} = SQL.fetch_all(writer, checkpoint)
    after
      SQL.release(writer, checkpoint)
    end

    assert [%{"name" => "changed"}] = DB.run(conn, "SELECT name FROM items WHERE id=1", [])
  end

  test "discarding a pool connection deletes its statement table", %{
    conn: conn,
    reader: reader,
    statements: statements,
    path: path
  } do
    sql = "SELECT name FROM items"
    assert [%{"name" => "one"}, %{"name" => "two"}] = DB.run(conn, sql, [])
    [{^sql, stmt}] = :ets.lookup(statements, sql)

    assert {:ok, {:reader, ^path}} =
             DB.Connection.terminate_worker(:error, conn, {:reader, path})

    # NimblePool owns the table, not the discarded worker. The pool stays alive.
    assert :undefined == :ets.info(statements)
    assert {:error, _} = SQL.multi_step(reader, stmt)
  end

  test "cancelled checkouts replace the connection without accumulating ETS tables", %{path: path} do
    pool = start_supervised!({NimblePool, worker: {DB.Connection, {:reader, path}}, pool_size: 1})

    tables = fn -> Enum.filter(:ets.all(), &(:ets.info(&1, :owner) == pool)) end
    assert length(tables.()) == 1

    for _ <- 1..3 do
      assert catch_throw(
               NimblePool.checkout!(pool, :checkout, fn _, conn ->
                 assert [%{"name" => "one"}] =
                          DB.run(conn, "SELECT name FROM items WHERE id=1", [])

                 throw(:cancelled)
               end)
             ) == :cancelled

      # The next checkout follows cancellation in the pool's mailbox, so no sleep is needed.
      assert [%{"name" => "two"}] =
               NimblePool.checkout!(pool, :checkout, fn _, conn ->
                 {DB.run(conn, "SELECT name FROM items WHERE id=2", []), :ok}
               end)

      assert length(tables.()) == 1
    end
  end
end

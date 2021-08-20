defmodule Testament.Repo.Migrations.CreateSnapshots do
    use Ecto.Migration

    def change do
        create table(:snapshots) do
            add :version,       :integer
            add :payload,       :binary
            add :stream_id,     :string
            add :stream_type,   :string
            timestamps()
        end

    end
end

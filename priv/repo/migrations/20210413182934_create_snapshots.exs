defmodule Testament.Repo.Migrations.CreateSnapshots do
    use Ecto.Migration

    def change do
        create table(:snapshots, primary_key: false) do
            add :uuid,          :uuid,      primary_key: true
            add :id,            :string
            add :version,       :integer
            add :data,          :binary
            timestamps()
        end
    end
end

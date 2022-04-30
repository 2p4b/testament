defmodule Testament.Repo.Migrations.CreateSnapshots do
    use Ecto.Migration

    def change do
        create table(:snapshots, primary_key: false) do
            add :uuid,          :uuid,      primary_key: true
            add :id,            :string
            add :type,          :string
            add :version,       :integer
            add :data,          :binary
        end
    end
end

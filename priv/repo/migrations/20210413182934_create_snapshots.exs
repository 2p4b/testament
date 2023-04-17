defmodule Testament.Repo.Migrations.CreateSnapshots do
    use Ecto.Migration

    def change do
        create table(:snapshots, primary_key: false) do
            add :id,            :string,    primary_key: true
            add :version,       :integer
            add :data,          :text
        end
    end
end

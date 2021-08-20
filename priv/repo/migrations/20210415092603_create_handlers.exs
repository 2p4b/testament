defmodule Testament.Repo.Migrations.CreateHandlers do
    use Ecto.Migration

    def change do
        create table(:handlers, primary_key: false) do
            add :name,          :string,      primary_key: true
            add :position,      :integer
            timestamps()
        end
    end
end

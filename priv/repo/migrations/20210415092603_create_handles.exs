defmodule Testament.Repo.Migrations.CreateHandles do
    use Ecto.Migration

    def change do
        create table(:handles, primary_key: false) do
            add :id,            :string,      primary_key: true
            add :position,      :integer
        end
    end
end
